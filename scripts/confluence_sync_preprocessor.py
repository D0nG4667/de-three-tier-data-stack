"""
Confluence Sync Preprocessor.

Script: confluence_sync_preprocessor.py
This script runs on the GitHub Actions runner before syncing documentation to Confluence.
It performs three key transformations to ensure Confluence compatibility:
1. Auto-reconciliation: Checks Confluence Cloud dynamically for existing pages with matching titles
   and automatically injects their Page IDs to prevent duplicate creation (HTTP 400 Bad Request) errors.
2. Converts LaTeX inline ($...$) and display ($$...$$) math equations into high-resolution inline SVG
   images hosted on CodeCogs. This resolves Confluence's lack of native LaTeX rendering.
3. Rewrites relative image paths (../assets/) to workspace root-relative paths (docs/assets/)
   to bypass confluence-md's strict path traversal security checks in directory mode.
"""

import os
import re
import urllib.parse
import urllib.request
import json
import base64
import sys
from typing import Match, Optional

def encode_math(math_text: str) -> str:
    """
    URL-encodes LaTeX math syntax and constructs a CodeCogs SVG rendering endpoint URL.
    
    Args:
        math_text: The raw LaTeX math block to render.
        
    Returns:
        A fully qualified URL string pointing to the CodeCogs SVG render API.
    """
    clean_math: str = math_text.strip()
    encoded: str = urllib.parse.quote(clean_math)
    return f'https://latex.codecogs.com/svg.image?{encoded}'

def replace_latex_in_text(text: str) -> str:
    """
    Scans a block of text and replaces standard inline LaTeX math ($...$) with SVG image tags.
    It excludes GitHub Action expressions (${{...}}) and shell variables to prevent false positives.
    
    Args:
        text: A single line of Markdown text.
        
    Returns:
        The line of text with inline math blocks translated to Markdown image syntax.
    """
    def inline_repl(match: Match[str]) -> str:
        math_content: str = match.group(1)
        # Use \\inline&space; prefix to ensure proper vertical baseline alignment in Confluence
        url: str = encode_math(f'\\inline&space;{math_content}')
        return f'![{math_content.strip()}]({url})'
    
    # Regex: matches $ not preceded/followed by another $ or a curly brace { (to protect ${{...}})
    return re.sub(r'(?<!\$)\$(?!\$)(?!\{)([^$\n]+?)(?<!\$)\$(?!\$)', inline_repl, text)

def replace_mermaid_blocks(text: str) -> str:
    """
    Scans a block of text, finds any fenced code blocks of type ```mermaid,
    URL-safe Base64-encodes their content, and replaces them with a dynamically
    rendered SVG image from the mermaid.ink CDN.
    
    Args:
        text: The full Markdown content of a file.
        
    Returns:
        The Markdown content with all Mermaid blocks replaced by SVG image links.
    """
    def mermaid_repl(match: Match[str]) -> str:
        mermaid_code: str = match.group(1).strip()
        # Encode to bytes, then base64
        graph_bytes = mermaid_code.encode("utf-8")
        base64_bytes = base64.urlsafe_b64encode(graph_bytes)
        base64_string = base64_bytes.decode("ascii").strip("=")
        
        url = f"https://mermaid.ink/img/{base64_string}?bgColor=white"
        return f"\n\n![Mermaid Diagram]({url})\n\n"

    # Regex matching ```mermaid ... ```
    return re.sub(r'```mermaid\s*\n(.*?)\n```', mermaid_repl, text, flags=re.DOTALL)

def replace_latex_and_paths(text: str) -> str:
    """
    Applies Mermaid diagram compilation, display math LaTeX conversion,
    line-by-line inline LaTeX conversion (excluding fenced code blocks),
    and relative image path rewriting to the document.
    
    Args:
        text: The full Markdown content of a file.
        
    Returns:
        The fully transformed Markdown content ready for Confluence sync.
    """
    # 1. Convert Mermaid blocks to rendered SVG image links
    text = replace_mermaid_blocks(text)

    # 2. Convert Display Math ($$...$$) to Markdown image blocks
    def display_repl(match: Match[str]) -> str:
        math_content: str = match.group(1)
        url: str = encode_math(math_content)
        return f'\n\n![{math_content.strip()}]({url})\n\n'
    
    text = re.sub(r'\$\$(.*?)\$\$', display_repl, text, flags=re.DOTALL)
    
    # 3. Rewrite relative image paths (../assets/ -> docs/assets/) to avoid path traversal blocks
    text = text.replace('../assets/', 'docs/assets/')
    
    # 3. Convert Inline Math line-by-line while ignoring code blocks
    lines = text.split('\n')
    in_code_block: bool = False
    for i, line in enumerate(lines):
        if line.strip().startswith('```'):
            in_code_block = not in_code_block
            continue
        if in_code_block:
            continue
        
        lines[i] = replace_latex_in_text(line)
        
    return '\n'.join(lines)

def extract_page_id(filepath: str, key: str = "confluence_page_id") -> Optional[str]:
    """
    Extracts the Confluence page ID from the YAML frontmatter of a Markdown file.
    
    Args:
        filepath: Path to the Markdown file.
        key: The YAML frontmatter key used to identify the page ID.
        
    Returns:
        The extracted page ID as a string, or None if the frontmatter key does not exist.
    """
    if not os.path.exists(filepath):
        return None
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    match = re.match(r'^---\s*\n(.*?)\n---\s*\n', content, re.DOTALL)
    if not match:
        return None
    
    frontmatter = match.group(1)
    for line in frontmatter.split('\n'):
        if line.strip().startswith(f"{key}:"):
            val = line.split(":", 1)[1].strip()
            return val.strip("'\"")
    return None

def extract_title(filepath: str) -> Optional[str]:
    """
    Extracts the first h1 title or frontmatter title from a markdown file.
    
    Args:
        filepath: Path to the Markdown file.
        
    Returns:
        The extracted page title, or None if not found.
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
        
    # Check frontmatter title first
    match = re.match(r'^---\s*\n(.*?)\n---\s*\n', content, re.DOTALL)
    if match:
        frontmatter = match.group(1)
        for line in frontmatter.split('\n'):
            if line.strip().startswith("title:"):
                return line.split(":", 1)[1].strip().strip("'\"")
                
    # Fallback to first markdown header #
    for line in content.split('\n'):
        if line.strip().startswith("# "):
            return line.strip("# ").strip()
    return None

def fetch_page_id(email: str, api_token: str, space_key: str, title: str) -> Optional[str]:
    """
    Queries Confluence API v2 for a page ID by space key and title.
    
    Args:
        email: The Atlassian account email.
        api_token: The Atlassian API token.
        space_key: The target Confluence space key.
        title: The page title to look up.
        
    Returns:
        The page ID string if found, else None.
    """
    encoded_title = urllib.parse.quote(title)
    url = f"https://uwe-bristol-air.atlassian.net/wiki/api/v2/pages?spaceKey={space_key}&title={encoded_title}"
    
    auth_str = f"{email}:{api_token}"
    auth_b64 = base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')
    
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Basic {auth_b64}")
    req.add_header("Accept", "application/json")
    
    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode('utf-8'))
            results = data.get("results", [])
            if results:
                return str(results[0].get("id"))
    except Exception as e:
        print(f"Warning: Failed to fetch Confluence ID for '{title}': {e}")
    return None

def inject_page_id(filepath: str, page_id: str, key: str = "confluence_page_id") -> None:
    """
    Injects the confluence_page_id into the frontmatter of the markdown file.
    
    Args:
        filepath: Path to the target Markdown file.
        page_id: The page ID to write.
        key: The frontmatter key to use.
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
        
    match = re.match(r'^---\s*\n(.*?)\n---\s*\n', content, re.DOTALL)
    if match:
        frontmatter = match.group(1)
        lines = frontmatter.split('\n')
        key_exists = False
        for idx, line in enumerate(lines):
            if line.strip().startswith(f"{key}:"):
                lines[idx] = f"{key}: \"{page_id}\""
                key_exists = True
                break
        if not key_exists:
            lines.append(f"{key}: \"{page_id}\"")
        new_frontmatter = '\n'.join(lines)
        new_content = f"---\n{new_frontmatter}\n---\n" + content[match.end():]
    else:
        new_content = f"---\n{key}: \"{page_id}\"\n---\n\n" + content
        
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)

def process_file(filepath: str) -> None:
    """
    Reads a Markdown file, dynamically checks Confluence Cloud for an existing page ID,
    executes LaTeX and image path preprocessing, and overwrites the file in place.
    
    Args:
        filepath: Absolute or relative path to the Markdown file.
    """
    email = os.environ.get("CONFLUENCE_EMAIL")
    api_token = os.environ.get("CONFLUENCE_API_TOKEN")
    space_key = os.environ.get("CONFLUENCE_SPACE_KEY", "uwebristol2026")
    
    # 1. On-the-fly reconciliation: check if the page ID is missing, and if so, query Confluence
    if email and api_token:
        current_page_id = extract_page_id(filepath)
        if not current_page_id:
            title = extract_title(filepath)
            if title:
                print(f"Checking Confluence for existing page title: '{title}'...")
                resolved_id = fetch_page_id(email, api_token, space_key, title)
                if resolved_id:
                    print(f"Found existing page ID: {resolved_id} for '{title}'. Injecting on the fly...")
                    inject_page_id(filepath, resolved_id)
                else:
                    print(f"No existing page found for '{title}'. Will let Confluence create a new page.")
    else:
        print("Reconciliation skipped (CONFLUENCE_EMAIL or CONFLUENCE_API_TOKEN env vars not set).")

    # 2. Proceed with LaTeX and relative image path preprocessing
    print(f"Pre-processing LaTeX & paths in: {filepath}")
    with open(filepath, 'r', encoding='utf-8') as f:
        content: str = f.read()
    
    updated_content: str = replace_latex_and_paths(content)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(updated_content)

def main() -> None:
    """
    Entry point parsing targets. Recursively processes directories or individual files.
    """
    if len(sys.argv) < 2:
        print("Usage: python confluence_sync_preprocessor.py <directory_or_file>")
        sys.exit(1)
    
    target: str = sys.argv[1]
    if os.path.isfile(target):
        process_file(target)
    elif os.path.isdir(target):
        for root, _, files in os.walk(target):
            for file in files:
                if file.endswith('.md'):
                    process_file(os.path.join(root, file))
    else:
        print(f"Target {target} not found.")
        sys.exit(1)

if __name__ == "__main__":
    main()
