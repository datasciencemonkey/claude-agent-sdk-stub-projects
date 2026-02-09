"""Generate PDF report from markdown with embedded charts."""
import os
import base64
import re
import markdown
from weasyprint import HTML, CSS

# Paths
REPORT_DIR = "reports"
MD_FILE = os.path.join(REPORT_DIR, "BJs_Executive_Report_Jan2026.md")
PDF_FILE = os.path.join(REPORT_DIR, "BJs_Executive_Report_Jan2026.pdf")
CHARTS_DIR = os.path.join(REPORT_DIR, "charts")

# Read markdown
with open(MD_FILE, "r") as f:
    md_content = f.read()

# Convert image paths to base64 embedded images
def embed_images(md_text, base_dir):
    """Replace image references with base64 embedded images."""
    def replace_image(match):
        alt_text = match.group(1)
        img_path = match.group(2)

        # Resolve path relative to report directory
        full_path = os.path.join(base_dir, img_path)

        if os.path.exists(full_path):
            with open(full_path, "rb") as img_file:
                img_data = base64.b64encode(img_file.read()).decode("utf-8")
            return f'![{alt_text}](data:image/png;base64,{img_data})'
        else:
            print(f"Warning: Image not found: {full_path}")
            return match.group(0)

    # Match markdown image syntax: ![alt](path)
    pattern = r'!\[([^\]]*)\]\(([^)]+)\)'
    return re.sub(pattern, replace_image, md_text)

print("Embedding images...")
md_with_images = embed_images(md_content, REPORT_DIR)

# Convert markdown to HTML
print("Converting markdown to HTML...")
md_converter = markdown.Markdown(extensions=['tables', 'fenced_code'])
html_body = md_converter.convert(md_with_images)

# Create full HTML document with styling
html_template = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>BJ's Wholesale Club - Executive Report</title>
</head>
<body>
{html_body}
</body>
</html>
"""

# CSS styling for professional PDF
css = CSS(string="""
@page {{
    size: A4;
    margin: 1.5cm 2cm;

    @top-center {{
        content: "BJ's Wholesale Club - Executive Report";
        font-size: 9pt;
        color: #666;
    }}

    @bottom-center {{
        content: "Page " counter(page) " of " counter(pages);
        font-size: 9pt;
        color: #666;
    }}
}}

@page :first {{
    @top-center {{
        content: "";
    }}
}}

body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
    font-size: 10pt;
    line-height: 1.5;
    color: #333;
    max-width: 100%;
}}

h1 {{
    color: #1E3A5F;
    font-size: 24pt;
    border-bottom: 3px solid #1E3A5F;
    padding-bottom: 10px;
    margin-top: 0;
}}

h2 {{
    color: #1E3A5F;
    font-size: 16pt;
    border-bottom: 2px solid #3498db;
    padding-bottom: 5px;
    margin-top: 25px;
    page-break-after: avoid;
}}

h3 {{
    color: #2c3e50;
    font-size: 12pt;
    margin-top: 20px;
    page-break-after: avoid;
}}

h4 {{
    color: #34495e;
    font-size: 11pt;
    margin-top: 15px;
}}

p {{
    margin: 8px 0;
    text-align: justify;
}}

table {{
    width: 100%;
    border-collapse: collapse;
    margin: 15px 0;
    font-size: 9pt;
    page-break-inside: avoid;
}}

th {{
    background-color: #1E3A5F;
    color: white;
    padding: 8px 10px;
    text-align: left;
    font-weight: 600;
}}

td {{
    padding: 6px 10px;
    border-bottom: 1px solid #ddd;
}}

tr:nth-child(even) {{
    background-color: #f8f9fa;
}}

tr:hover {{
    background-color: #e8f4f8;
}}

img {{
    max-width: 100%;
    height: auto;
    display: block;
    margin: 15px auto;
    border: 1px solid #ddd;
    border-radius: 4px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
}}

strong {{
    color: #1E3A5F;
}}

code {{
    background-color: #f4f4f4;
    padding: 2px 6px;
    border-radius: 3px;
    font-family: 'Monaco', 'Menlo', monospace;
    font-size: 9pt;
}}

hr {{
    border: none;
    border-top: 1px solid #ddd;
    margin: 20px 0;
}}

ul, ol {{
    margin: 10px 0;
    padding-left: 25px;
}}

li {{
    margin: 5px 0;
}}

blockquote {{
    border-left: 4px solid #3498db;
    margin: 15px 0;
    padding: 10px 20px;
    background-color: #f8f9fa;
    font-style: italic;
}}

/* Executive Summary styling */
h1 + h2 + h3 {{
    margin-top: 10px;
}}

/* Key metrics table */
table:first-of-type {{
    background-color: #f8f9fa;
}}

/* Urgent callouts */
p:has(strong:contains("URGENT")) {{
    background-color: #ffeaea;
    border-left: 4px solid #e74c3c;
    padding: 10px;
    margin: 15px 0;
}}

/* Insight callouts */
p:has(strong:contains("Insight")) {{
    background-color: #e8f6ff;
    border-left: 4px solid #3498db;
    padding: 10px;
    margin: 15px 0;
}}

/* Print optimizations */
h2, h3 {{
    page-break-after: avoid;
}}

table, img {{
    page-break-inside: avoid;
}}

/* Section breaks */
hr {{
    page-break-after: always;
}}
""")

# Generate PDF
print("Generating PDF...")
html = HTML(string=html_template)
html.write_pdf(PDF_FILE, stylesheets=[css])

print(f"\nPDF generated successfully!")
print(f"Location: {os.path.abspath(PDF_FILE)}")
print(f"File size: {os.path.getsize(PDF_FILE) / 1024:.1f} KB")
