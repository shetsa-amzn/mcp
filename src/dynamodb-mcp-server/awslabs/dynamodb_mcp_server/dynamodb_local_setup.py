"""DynamoDB Local setup for Data Model Validation."""

import subprocess
import socket
import time
import urllib.request
import shutil
from loguru import logger
from typing import Optional


def is_docker_available() -> bool:
    """Check if Docker is available and functional."""
    try:
        docker_path = shutil.which("docker")
        if not docker_path:
            return False
        subprocess.run([docker_path, "--version"], capture_output=True, check=True, timeout=5)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
        logger.debug(f"Docker not available: {e}")
        return False
    

def find_available_port(start_port: int = 8000) -> int:
    """Find the first available port starting from the given port."""
    port = start_port
    while port < 65535:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                if sock.connect_ex(('localhost', port)) != 0:
                    return port
        except OSError as e:
            logger.debug(f"Error checking port {port}: {e}")
        port += 1
    raise RuntimeError("No available ports found in range 8000-65534")


def get_running_container_endpoint() -> Optional[str]:
    """Check if our container is running and return its endpoint."""
    container_name = "dynamodb-local-mcp-server"
    
    try:
        docker_path = shutil.which("docker")
        if not docker_path:
            return None
            
        # Check if container is running and get port mapping
        check_cmd = [docker_path, "ps", "--format", "{{.Ports}}", "-f", f"name={container_name}"]
        result = subprocess.run(check_cmd, capture_output=True, text=True, check=True)
        
        if result.stdout.strip():
            # Parse port from output like "0.0.0.0:8001->8000/tcp"
            ports_output = result.stdout.strip()
            if "->" in ports_output:
                host_port = ports_output.split("->")[0].split(":")[-1]
                endpoint = f"http://localhost:{host_port}"
                logger.info(f"Found existing DynamoDB Local container at {endpoint}")
                return endpoint
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.debug(f"Error checking for existing container: {e}")
    
    return None


def start_docker_container(port: int) -> str:
    """Start DynamoDB Local Docker container."""
    container_name = "dynamodb-local-mcp-server"
    
    docker_path = shutil.which("docker")
    if not docker_path:
        raise RuntimeError("Docker executable not found in PATH")
    
    # Start fresh container
    cmd = [
        docker_path, "run", "-d", "--name", container_name,
        "-p", f"{port}:8000",
        "amazon/dynamodb-local"
    ]
    
    try:
        logger.info(f"Starting DynamoDB Local container on port {port}")
        subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to start Docker container: {e.stderr}")
    
    endpoint = f"http://localhost:{port}"
    
    # Wait for DynamoDB Local to be ready (up to 30 seconds)
    for i in range(10):
        try:
            urllib.request.urlopen(endpoint, timeout=2)
            logger.info(f"DynamoDB Local ready at {endpoint}")
            return endpoint
        except (urllib.error.URLError, OSError) as e:
            if i == 9:  # Last attempt
                raise RuntimeError(
                    f"DynamoDB Local failed to start at {endpoint} after 10 seconds. "
                    f"Check Docker logs: docker logs {container_name}. Last error: {e}"
                )
            logger.debug(f"DynamoDB Local not ready, retrying in 1s (attempt {i+1}/10)")
            time.sleep(1)
    
    raise RuntimeError(f"Unexpected error waiting for DynamoDB Local at {endpoint}")


def setup_dynamodb_local() -> str:
    """
    Setup DynamoDB Local environment.
    
    Returns:
        str: DynamoDB Local endpoint URL
        
    Raises:
        RuntimeError: If Docker is not available or setup fails
    """
    # Check if our container is already running
    existing_endpoint = get_running_container_endpoint()
    if existing_endpoint:
        return existing_endpoint
    
    # Check prerequisites
    has_docker = is_docker_available()
    
    if not has_docker:
        raise RuntimeError(
            "Docker is not available. Please install Docker Desktop from https://docker.com/products/docker-desktop "
        )
    
    # Find available port
    try:
        port = find_available_port(8000)
    except RuntimeError as e:
        raise RuntimeError(f"Cannot find available port: {e}")
    
    # Setup using Docker
    return start_docker_container(port)
