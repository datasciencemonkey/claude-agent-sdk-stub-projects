"""Generate self-contained HTML report with embedded charts."""
import os
import base64
import re
import markdown

# Paths
REPORT_DIR = "reports"
MD_FILE = os.path.join(REPORT_DIR, "BJs_Executive_Report_Jan2026.md")
HTML_FILE = os.path.join(REPORT_DIR, "BJs_Executive_Report_Jan2026.html")
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
md_converter = markdown.Markdown(extensions=['tables', 'fenced_code', 'toc'])
html_body = md_converter.convert(md_with_images)

# Create full HTML document with professional styling
html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BJ's Wholesale Club - Executive Business Intelligence Report - January 2026</title>
    <style>
        /* Reset and base */
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            font-size: 11pt;
            line-height: 1.6;
            color: #333;
            background: #f5f5f5;
        }}

        .container {{
            max-width: 1000px;
            margin: 0 auto;
            background: white;
            box-shadow: 0 0 20px rgba(0,0,0,0.1);
        }}

        .content {{
            padding: 40px 60px;
        }}

        /* Typography */
        h1 {{
            color: #1E3A5F;
            font-size: 28pt;
            font-weight: 700;
            border-bottom: 4px solid #1E3A5F;
            padding-bottom: 15px;
            margin-bottom: 5px;
        }}

        h1 + h2 {{
            color: #3498db;
            font-size: 16pt;
            font-weight: 400;
            border: none;
            margin-top: 5px;
            margin-bottom: 5px;
        }}

        h1 + h2 + h3 {{
            color: #666;
            font-size: 12pt;
            font-weight: 400;
            margin-bottom: 30px;
        }}

        h2 {{
            color: #1E3A5F;
            font-size: 18pt;
            font-weight: 600;
            border-bottom: 2px solid #3498db;
            padding-bottom: 8px;
            margin-top: 40px;
            margin-bottom: 20px;
            page-break-after: avoid;
        }}

        h3 {{
            color: #2c3e50;
            font-size: 13pt;
            font-weight: 600;
            margin-top: 25px;
            margin-bottom: 15px;
            page-break-after: avoid;
        }}

        h4 {{
            color: #34495e;
            font-size: 11pt;
            font-weight: 600;
            margin-top: 20px;
            margin-bottom: 10px;
        }}

        p {{
            margin: 12px 0;
            text-align: justify;
        }}

        /* Tables */
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            font-size: 10pt;
            page-break-inside: avoid;
        }}

        th {{
            background: linear-gradient(135deg, #1E3A5F 0%, #2c5282 100%);
            color: white;
            padding: 12px 15px;
            text-align: left;
            font-weight: 600;
            font-size: 9pt;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        td {{
            padding: 10px 15px;
            border-bottom: 1px solid #e0e0e0;
        }}

        tr:nth-child(even) {{
            background-color: #f8fafc;
        }}

        tr:hover {{
            background-color: #e8f4f8;
        }}

        /* First column often has labels */
        td:first-child {{
            font-weight: 500;
            color: #1E3A5F;
        }}

        /* Images */
        img {{
            max-width: 100%;
            height: auto;
            display: block;
            margin: 25px auto;
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }}

        /* Lists */
        ul, ol {{
            margin: 15px 0;
            padding-left: 30px;
        }}

        li {{
            margin: 8px 0;
        }}

        /* Emphasis */
        strong {{
            color: #1E3A5F;
            font-weight: 600;
        }}

        /* Code */
        code {{
            background-color: #f4f4f4;
            padding: 3px 8px;
            border-radius: 4px;
            font-family: 'SF Mono', 'Monaco', 'Menlo', monospace;
            font-size: 9pt;
            color: #e74c3c;
        }}

        /* Horizontal rules */
        hr {{
            border: none;
            border-top: 1px solid #e0e0e0;
            margin: 30px 0;
        }}

        /* Blockquotes */
        blockquote {{
            border-left: 4px solid #3498db;
            margin: 20px 0;
            padding: 15px 25px;
            background-color: #f8fafc;
            font-style: italic;
            color: #555;
        }}

        /* Special callout styles */
        p strong:first-child {{
            display: inline;
        }}

        /* Key Highlights section */
        h3:contains("Key Highlights") + table {{
            background: linear-gradient(135deg, #f8fafc 0%, #e8f4f8 100%);
        }}

        /* Print styles */
        @media print {{
            body {{
                background: white;
            }}

            .container {{
                box-shadow: none;
                max-width: 100%;
            }}

            .content {{
                padding: 20px 40px;
            }}

            h2 {{
                page-break-after: avoid;
            }}

            table, img {{
                page-break-inside: avoid;
            }}

            h1 {{
                font-size: 24pt;
            }}

            h2 {{
                font-size: 16pt;
            }}

            h3 {{
                font-size: 12pt;
            }}
        }}

        @page {{
            size: A4;
            margin: 2cm;
        }}

        /* Executive summary box */
        h2:first-of-type {{
            margin-top: 20px;
        }}

        /* Footer */
        .footer {{
            text-align: center;
            padding: 20px;
            color: #666;
            font-size: 9pt;
            border-top: 1px solid #e0e0e0;
            margin-top: 40px;
        }}

        /* Header bar */
        .header-bar {{
            background: linear-gradient(135deg, #1E3A5F 0%, #2c5282 100%);
            color: white;
            padding: 15px 60px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}

        .header-bar .logo {{
            font-size: 14pt;
            font-weight: 700;
        }}

        .header-bar .date {{
            font-size: 10pt;
            opacity: 0.9;
        }}

        /* Recommendation sections */
        h3:contains("Immediate"), h3:contains("Short-term"), h3:contains("Long-term") {{
            background: #f0f7ff;
            padding: 10px 15px;
            border-radius: 5px;
            margin-left: -15px;
            padding-left: 15px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header-bar">
            <div class="logo">BJ's Wholesale Club</div>
            <div class="date">Executive Report | January 2026</div>
        </div>
        <div class="content">
            {html_body}
        </div>
        <div class="footer">
            <p>This report was generated from data in <code>serverless_9cefok_catalog.sgfs</code></p>
            <p>Generated: January 21, 2026 | Confidential - For Internal Use Only</p>
        </div>
    </div>

    <script>
        // Print button functionality
        function printReport() {{
            window.print();
        }}
    </script>
</body>
</html>
"""

# Write HTML file
with open(HTML_FILE, "w") as f:
    f.write(html_template)

print(f"\nHTML report generated successfully!")
print(f"Location: {os.path.abspath(HTML_FILE)}")
print(f"File size: {os.path.getsize(HTML_FILE) / 1024:.1f} KB")
print(f"\nTo create PDF:")
print(f"  1. Open the HTML file in Chrome/Safari/Edge")
print(f"  2. Press Cmd+P (Mac) or Ctrl+P (Windows)")
print(f"  3. Select 'Save as PDF' as the destination")
print(f"  4. Click Save")
