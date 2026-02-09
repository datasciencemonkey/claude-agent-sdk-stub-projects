"""PDF tools - Generate synthetic PDF documents for RAG/unstructured data use cases."""

import asyncio
from typing import Any, Dict, Literal

from databricks_tools_core.pdf import DocSize
from databricks_tools_core.pdf import generate_pdf_documents as _generate_pdf_documents

from ..server import mcp


@mcp.tool
def generate_pdf_documents(
    catalog: str,
    schema: str,
    description: str,
    count: int,
    volume: str = 'raw_data',
    folder: str = 'pdf_documents',
    doc_size: Literal['SMALL', 'MEDIUM', 'LARGE'] = 'MEDIUM',
    overwrite_folder: bool = False,
) -> Dict[str, Any]:
    """
    Generate synthetic PDF documents for RAG/unstructured data use cases.

    This tool generates realistic PDF documents using a 2-step process:
    1. Uses LLM to generate diverse document specifications
    2. Generates HTML content and converts to PDF in parallel

    Each PDF also gets a companion JSON file with a question/guideline pair
    for RAG evaluation purposes.

    Args:
        catalog: Unity Catalog name
        schema: Schema name
        description: Detailed description of what PDFs should contain.
            Be specific about the domain, document types, and content.
            Example: "Technical documentation for a cloud infrastructure platform
            including API guides, troubleshooting manuals, and security policies."
        count: Number of PDFs to generate (recommended: 5-20)
        volume: Volume name (created if not exists). Default: "raw_data"
        folder: Folder within volume (e.g., "technical_docs"). Default: "pdf_documents"
        doc_size: Size of documents to generate. Default: "MEDIUM"
            - "SMALL": ~1 page, concise content
            - "MEDIUM": ~4-6 pages, comprehensive coverage (default)
            - "LARGE": ~10+ pages, exhaustive documentation
        overwrite_folder: If True, delete existing folder content first (default: False)

    Returns:
        Dictionary with:
        - success: True if all PDFs generated successfully
        - volume_path: Path to the volume folder containing PDFs
        - pdfs_generated: Number of PDFs successfully created
        - pdfs_failed: Number of PDFs that failed
        - errors: List of error messages if any

    Example:
        >>> generate_pdf_documents(
        ...     catalog="my_catalog",
        ...     schema="my_schema",
        ...     description="HR policy documents including employee handbook, "
        ...                 "leave policies, code of conduct, and benefits guide",
        ...     count=10,
        ...     doc_size="SMALL"
        ... )
        {
            "success": True,
            "volume_path": "/Volumes/my_catalog/my_schema/raw_data/pdf_documents",
            "pdfs_generated": 10,
            "pdfs_failed": 0,
            "errors": []
        }

    Environment Variables Required:
        For Databricks LLM (default):
        - DATABRICKS_TOKEN or DATABRICKS_API_KEY
        - DATABRICKS_HOST
        - DATABRICKS_MODEL (model serving endpoint name)

        For Azure OpenAI:
        - LLM_PROVIDER=AZURE
        - AZURE_API_KEY or AZURE_OPENAI_API_KEY
        - AZURE_API_BASE or AZURE_OPENAI_ENDPOINT
        - AZURE_OPENAI_DEPLOYMENT (deployment name)
    """
    # Convert string to DocSize enum
    size_enum = DocSize(doc_size)

    # Run the async function synchronously
    result = asyncio.run(
        _generate_pdf_documents(
            catalog=catalog,
            schema=schema,
            description=description,
            count=count,
            volume=volume,
            folder=folder,
            doc_size=size_enum,
            overwrite_folder=overwrite_folder,
            max_workers=4,
        )
    )

    return {
        'success': result.success,
        'volume_path': result.volume_path,
        'pdfs_generated': result.pdfs_generated,
        'pdfs_failed': result.pdfs_failed,
        'errors': result.errors,
    }
