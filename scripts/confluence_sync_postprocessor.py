"""
Confluence Sync Postprocessor.

Script: confluence_sync_postprocessor.py
This script runs on the GitHub Actions runner after confluence-md finishes execution.
It extracts newly generated Confluence Page IDs from the pre-processed directory
and merges them back into the clean backup folder (preserving standard LaTeX and relative image paths).
The workflow then replaces the modified directory with the merged backup before committing to Git.
"""

import os
import re
from typing import Optional

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
        content: str = f.read()
    
    # Match the frontmatter block at the very start of the file
    match = re.match(r'^---\s*\n(.*?)\n---\s*\n', content, re.DOTALL)
    if not match:
        return None
    
    frontmatter: str = match.group(1)
    for line in frontmatter.split('\n'):
        if line.strip().startswith(f"{key}:"):
            val: str = line.split(":", 1)[1].strip()
            # Clean quotes around the page ID string
            return val.strip("'\"")
    return None

def inject_page_id(filepath: str, page_id: str, key: str = "confluence_page_id") -> None:
    """
    Injects or updates the Confluence page ID in the frontmatter of a clean Markdown file.
    
    Args:
        filepath: Path to the target Markdown file.
        page_id: The numeric page ID to insert.
        key: The YAML frontmatter key for the page ID.
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content: str = f.read()
    
    match = re.match(r'^---\s*\n(.*?)\n---\s*\n', content, re.DOTALL)
    if match:
        # If frontmatter exists, update or append the key
        frontmatter: str = match.group(1)
        lines = frontmatter.split('\n')
        key_exists: bool = False
        for idx, line in enumerate(lines):
            if line.strip().startswith(f"{key}:"):
                lines[idx] = f"{key}: \"{page_id}\""
                key_exists = True
                break
        if not key_exists:
            lines.append(f"{key}: \"{page_id}\"")
        
        new_frontmatter: str = '\n'.join(lines)
        new_content: str = f"---\n{new_frontmatter}\n---\n" + content[match.end():]
    else:
        # If no frontmatter exists, construct a new YAML block at the top
        new_content = f"---\n{key}: \"{page_id}\"\n---\n\n" + content
        
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)

def main() -> None:
    """
    Walks the modified docs/ folder, extracts auto-created Page IDs,
    and injects them back into the clean backup folder.
    """
    docs_dir: str = "docs"
    backup_dir: str = "docs_backup"
    
    if not os.path.exists(backup_dir):
        print(f"Error: Backup directory {backup_dir} not found.")
        return
        
    for root, _, files in os.walk(docs_dir):
        for file in files:
            if file.endswith('.md'):
                modified_path: str = os.path.join(root, file)
                rel_path: str = os.path.relpath(modified_path, docs_dir)
                backup_path: str = os.path.join(backup_dir, rel_path)
                
                if os.path.exists(backup_path):
                    page_id: Optional[str] = extract_page_id(modified_path)
                    if page_id:
                        print(f"Restoring {rel_path} with page ID: {page_id}")
                        inject_page_id(backup_path, page_id)
                    else:
                        print(f"Restoring {rel_path} (no page ID changes)")
                else:
                    print(f"Warning: Backup file for {rel_path} not found.")

if __name__ == "__main__":
    main()
