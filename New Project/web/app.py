"""Web interface for the distributed file storage system."""

import asyncio
from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path
import tempfile
import os
from typing import List, Dict, Any
import io

from dfs.client.client import DFSClient
from dfs.utils.logging import setup_logging, get_logger


# Initialize FastAPI app
app = FastAPI(title="Distributed File Storage", version="1.0.0")

# Setup logging
setup_logging("INFO")
logger = get_logger("web")

# Initialize DFS client
DFS_NODES = os.getenv("DFS_NODES", "localhost:8001,localhost:8002,localhost:8003").split(",")
dfs_client = DFSClient(DFS_NODES)

# Setup templates
templates = Jinja2Templates(directory="web/templates")

# Mount static files
app.mount("/static", StaticFiles(directory="web/static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Main page."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/files")
async def list_files(prefix: str = "", limit: int = 100) -> List[Dict[str, Any]]:
    """API endpoint to list files."""
    try:
        files = await dfs_client.list_files(prefix, limit)
        return files
    except Exception as e:
        logger.error(f"Failed to list files: {e}")
        raise HTTPException(status_code=500, detail="Failed to list files")


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    """API endpoint to upload files."""
    try:
        # Read file content
        content = await file.read()
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(content)
            temp_path = temp_file.name
        
        try:
            # Upload to DFS
            success = await dfs_client.upload_file(temp_path)
            
            if success:
                return {"success": True, "message": f"File {file.filename} uploaded successfully"}
            else:
                raise HTTPException(status_code=500, detail="Upload failed")
                
        finally:
            # Clean up temporary file
            os.unlink(temp_path)
            
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@app.get("/api/download/{filename}")
async def download_file(filename: str):
    """API endpoint to download files."""
    try:
        # Create temporary file for download
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
        
        # Download from DFS
        success = await dfs_client.download_file(filename, temp_path)
        
        if not success:
            os.unlink(temp_path)
            raise HTTPException(status_code=404, detail="File not found")
        
        # Return file as streaming response
        def file_generator():
            with open(temp_path, 'rb') as f:
                while True:
                    chunk = f.read(8192)
                    if not chunk:
                        break
                    yield chunk
            # Clean up after streaming
            os.unlink(temp_path)
        
        return StreamingResponse(
            file_generator(),
            media_type='application/octet-stream',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        logger.error(f"Download failed: {e}")
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")


@app.delete("/api/files/{filename}")
async def delete_file(filename: str):
    """API endpoint to delete files."""
    try:
        success = await dfs_client.delete_file(filename)
        
        if success:
            return {"success": True, "message": f"File {filename} deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="File not found")
            
    except Exception as e:
        logger.error(f"Delete failed: {e}")
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")


@app.get("/api/files/{filename}/info")
async def get_file_info(filename: str) -> Dict[str, Any]:
    """API endpoint to get file information."""
    try:
        file_info = await dfs_client.get_file_info(filename)
        
        if not file_info:
            raise HTTPException(status_code=404, detail="File not found")
        
        return file_info
        
    except Exception as e:
        logger.error(f"Get file info failed: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get file info: {str(e)}")


@app.get("/api/cluster/status")
async def get_cluster_status() -> Dict[str, Any]:
    """API endpoint to get cluster status."""
    try:
        status = await dfs_client.get_cluster_status()
        
        if not status:
            raise HTTPException(status_code=500, detail="Failed to get cluster status")
        
        return status
        
    except Exception as e:
        logger.error(f"Get cluster status failed: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get cluster status: {str(e)}")


@app.post("/api/cluster/rebalance")
async def rebalance_cluster():
    """API endpoint to trigger cluster rebalancing."""
    try:
        success = await dfs_client.rebalance_cluster()
        
        if success:
            return {"success": True, "message": "Cluster rebalancing completed"}
        else:
            raise HTTPException(status_code=500, detail="Rebalancing failed")
            
    except Exception as e:
        logger.error(f"Rebalance failed: {e}")
        raise HTTPException(status_code=500, detail=f"Rebalancing failed: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)