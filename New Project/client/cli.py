"""Command-line interface for the distributed file storage system."""

import asyncio
import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel
from rich import print as rprint
from datetime import datetime
from pathlib import Path

from dfs.client.client import DFSClient
from dfs.utils.logging import setup_logging


console = Console()


def format_size(size_bytes: int) -> str:
    """Format file size in human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"


def format_timestamp(timestamp: int) -> str:
    """Format timestamp in human readable format."""
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


@click.group()
@click.option('--nodes', default='localhost:8001,localhost:8002,localhost:8003',
              help='Comma-separated list of node addresses')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.pass_context
def cli(ctx, nodes, verbose):
    """Distributed File Storage System CLI"""
    ctx.ensure_object(dict)
    ctx.obj['nodes'] = nodes.split(',')
    
    if verbose:
        setup_logging("DEBUG")
    else:
        setup_logging("WARNING")


@cli.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('--metadata', '-m', multiple=True, 
              help='Metadata key=value pairs (can be used multiple times)')
@click.pass_context
def upload(ctx, file_path, metadata):
    """Upload a file to the distributed storage."""
    
    async def _upload():
        client = DFSClient(ctx.obj['nodes'])
        
        # Parse metadata
        meta_dict = {}
        for item in metadata:
            if '=' in item:
                key, value = item.split('=', 1)
                meta_dict[key] = value
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task(f"Uploading {Path(file_path).name}...", total=None)
            
            success = await client.upload_file(file_path, meta_dict)
            
            if success:
                rprint(f"[green]✓[/green] Successfully uploaded {Path(file_path).name}")
            else:
                rprint(f"[red]✗[/red] Failed to upload {Path(file_path).name}")
                raise click.ClickException("Upload failed")
    
    asyncio.run(_upload())


@cli.command()
@click.argument('filename')
@click.option('--output', '-o', help='Output file path')
@click.pass_context
def download(ctx, filename, output):
    """Download a file from the distributed storage."""
    
    async def _download():
        client = DFSClient(ctx.obj['nodes'])
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task(f"Downloading {filename}...", total=None)
            
            success = await client.download_file(filename, output)
            
            if success:
                output_file = output or filename
                rprint(f"[green]✓[/green] Successfully downloaded to {output_file}")
            else:
                rprint(f"[red]✗[/red] Failed to download {filename}")
                raise click.ClickException("Download failed")
    
    asyncio.run(_download())


@cli.command()
@click.argument('filename')
@click.pass_context
def delete(ctx, filename):
    """Delete a file from the distributed storage."""
    
    async def _delete():
        client = DFSClient(ctx.obj['nodes'])
        
        if not click.confirm(f"Are you sure you want to delete '{filename}'?"):
            return
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task(f"Deleting {filename}...", total=None)
            
            success = await client.delete_file(filename)
            
            if success:
                rprint(f"[green]✓[/green] Successfully deleted {filename}")
            else:
                rprint(f"[red]✗[/red] Failed to delete {filename}")
                raise click.ClickException("Delete failed")
    
    asyncio.run(_delete())


@cli.command()
@click.option('--prefix', '-p', default='', help='Filter files by prefix')
@click.option('--limit', '-l', default=100, help='Maximum number of files to list')
@click.pass_context
def list(ctx, prefix, limit):
    """List files in the distributed storage."""
    
    async def _list():
        client = DFSClient(ctx.obj['nodes'])
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Fetching file list...", total=None)
            
            files = await client.list_files(prefix, limit)
        
        if not files:
            rprint("[yellow]No files found[/yellow]")
            return
        
        table = Table(title="Distributed Storage Files")
        table.add_column("Filename", style="cyan", no_wrap=True)
        table.add_column("Size", justify="right")
        table.add_column("Created", style="dim")
        table.add_column("Replicas", justify="center")
        table.add_column("Checksum", style="dim", max_width=16)
        
        for file_info in files:
            table.add_row(
                file_info['filename'],
                format_size(file_info['size']),
                format_timestamp(file_info['created_at']),
                str(len(file_info['replicas'])),
                file_info['checksum'][:12] + "..."
            )
        
        console.print(table)
    
    asyncio.run(_list())


@cli.command()
@click.argument('filename')
@click.pass_context
def info(ctx, filename):
    """Get detailed information about a file."""
    
    async def _info():
        client = DFSClient(ctx.obj['nodes'])
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task(f"Getting info for {filename}...", total=None)
            
            file_info = await client.get_file_info(filename)
        
        if not file_info:
            rprint(f"[red]File '{filename}' not found[/red]")
            return
        
        info_panel = Panel.fit(
            f"""[bold]Filename:[/bold] {file_info['filename']}
[bold]Size:[/bold] {format_size(file_info['size'])}
[bold]Checksum:[/bold] {file_info['checksum']}
[bold]Created:[/bold] {format_timestamp(file_info['created_at'])}
[bold]Modified:[/bold] {format_timestamp(file_info['modified_at'])}
[bold]Version:[/bold] {file_info['version']}
[bold]Replicas:[/bold] {len(file_info['replicas'])} ({', '.join(file_info['replicas'])})
[bold]Metadata:[/bold] {file_info['metadata'] if file_info['metadata'] else 'None'}""",
            title="File Information",
            border_style="blue"
        )
        
        console.print(info_panel)
    
    asyncio.run(_info())


@cli.command()
@click.pass_context
def status(ctx):
    """Get cluster status information."""
    
    async def _status():
        client = DFSClient(ctx.obj['nodes'])
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Getting cluster status...", total=None)
            
            status = await client.get_cluster_status()
        
        if not status:
            rprint("[red]Failed to get cluster status[/red]")
            return
        
        # Cluster overview
        overview_panel = Panel.fit(
            f"""[bold]Total Files:[/bold] {status['total_files']}
[bold]Total Size:[/bold] {format_size(status['total_size'])}
[bold]Active Nodes:[/bold] {len([n for n in status['nodes'] if n['healthy']])}/{len(status['nodes'])}""",
            title="Cluster Overview",
            border_style="green"
        )
        
        console.print(overview_panel)
        console.print()
        
        # Node details
        table = Table(title="Node Status")
        table.add_column("Node ID", style="cyan")
        table.add_column("Address", style="blue")
        table.add_column("Status", justify="center")
        table.add_column("Storage Used", justify="right")
        table.add_column("Storage Available", justify="right")
        table.add_column("Last Seen", style="dim")
        
        for node in status['nodes']:
            status_icon = "[green]●[/green]" if node['healthy'] else "[red]●[/red]"
            status_text = "Healthy" if node['healthy'] else "Unhealthy"
            
            table.add_row(
                node['node_id'],
                f"{node['address']}:{node['port']}",
                f"{status_icon} {status_text}",
                format_size(node['storage_used']),
                format_size(node['storage_available']),
                format_timestamp(node['last_seen'])
            )
        
        console.print(table)
    
    asyncio.run(_status())


@cli.command()
@click.pass_context
def rebalance(ctx):
    """Trigger cluster rebalancing."""
    
    async def _rebalance():
        client = DFSClient(ctx.obj['nodes'])
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Rebalancing cluster...", total=None)
            
            success = await client.rebalance_cluster()
        
        if success:
            rprint("[green]✓[/green] Cluster rebalancing completed successfully")
        else:
            rprint("[red]✗[/red] Cluster rebalancing failed")
            raise click.ClickException("Rebalancing failed")
    
    asyncio.run(_rebalance())


if __name__ == '__main__':
    cli()