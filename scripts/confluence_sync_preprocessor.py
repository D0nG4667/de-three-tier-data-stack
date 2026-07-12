"""
Confluence Sync Preprocessor.

Script: confluence_sync_preprocessor.py
This script runs on the GitHub Actions runner before syncing documentation to Confluence.
It performs two key transformations to ensure Confluence compatibility:
1. Converts LaTeX inline ($...$) and display ($$...$$) math equations into high-resolution inline SVG
   images hosted on CodeCogs. This resolves Confluence's lack of native LaTeX rendering.
2. Rewrites relative image paths (../assets/) to workspace root-relative paths (docs/assets/)
   to bypass confluence-md's strict path traversal security checks in directory mode.
"""

import os
import re
import urllib.parse
import sys
from typing import Match

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

def replace_latex_and_paths(text: str) -> str:
    """
    Applies display math LaTeX conversion, line-by-line inline LaTeX conversion
    (excluding fenced code blocks), and relative image path rewriting to the document.
    
    Args:
        text: The full Markdown content of a file.
        
    Returns:
        The fully transformed Markdown content ready for Confluence sync.
    """
    # 1. Convert Display Math ($$...$$) to Markdown image blocks
    def display_repl(match: Match[str]) -> str:
        math_content: str = match.group(1)
        url: str = encode_math(math_content)
        return f'\n\n![{math_content.strip()}]({url})\n\n'
    
    text = re.sub(r'\$\$(.*?)\$\$', display_repl, text, flags=re.DOTALL)
    
    # 2. Rewrite relative image paths (../assets/ -> assets/) to align with attachments_base: "docs/"
    text = text.replace('../assets/', 'assets/')
    
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

def process_file(filepath: str) -> None:
    """
    Reads a Markdown file, executes LaTeX and image path preprocessing,
    and overwrites the file in place with the modified content.
    
    Args:
        filepath: Absolute or relative path to the Markdown file.
    """
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
