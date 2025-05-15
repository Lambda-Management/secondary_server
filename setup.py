import os
import argparse
import subprocess
import sys

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Setup Data Server')
    parser.add_argument('--port', type=int, default=8080, help='Port to run the data server on')
    parser.add_argument('--main-url', type=str, default='http://localhost:8000', help='URL of the main server')
    parser.add_argument('--server-id', type=str, default='secondary', choices=['primary', 'secondary'], help='Server ID')
    
    args = parser.parse_args()
    
    print("=== Data Server Setup ===")
    print("\nVerifying requirements...")
    
    # Check if Python is installed
    try:
        python_version = subprocess.check_output([sys.executable, '--version']).decode().strip()
        print(f"✅ Python detected: {python_version}")
    except:
        print("❌ Python not found. Please install Python 3.8 or newer.")
        return 1
        
    # Create data_cache directory if it doesn't exist
    if not os.path.exists('data_cache'):
        os.makedirs('data_cache')
        print("✅ Created data_cache directory")
        
    # Install dependencies
    print("\nInstalling dependencies...")
    try:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'])
        print("✅ Dependencies installed successfully")
    except subprocess.CalledProcessError:
        print("❌ Failed to install dependencies")
        return 1
        
    # Create .env file
    print("\nCreating .env file...")
    with open('.env', 'w') as f:
        f.write(f"SERVER_ID={args.server_id}\n")
        f.write(f"SERVER_PORT={args.port}\n")
        f.write(f"MAIN_SERVER_URL={args.main_url}\n")
    print("✅ Created .env file")
    
    # Create startup scripts
    print("\nCreating startup scripts...")
    
    # Windows batch file
    with open('start_server.bat', 'w') as f:
        f.write(f'@echo off\n')
        f.write(f'echo Starting Data Server...\n')
        f.write(f'set SERVER_ID={args.server_id}\n')
        f.write(f'set SERVER_PORT={args.port}\n')
        f.write(f'set MAIN_SERVER_URL={args.main_url}\n')
        f.write(f'python data_service.py\n')
        f.write(f'pause\n')
    print("✅ Created start_server.bat")
    
    # Linux/Mac shell script
    with open('start_server.sh', 'w') as f:
        f.write(f'#!/bin/bash\n')
        f.write(f'echo "Starting Data Server..."\n')
        f.write(f'export SERVER_ID={args.server_id}\n')
        f.write(f'export SERVER_PORT={args.port}\n')
        f.write(f'export MAIN_SERVER_URL={args.main_url}\n')
        f.write(f'python data_service.py\n')
    
    # Make shell script executable on Linux/Mac
    try:
        import stat
        st = os.stat('start_server.sh')
        os.chmod('start_server.sh', st.st_mode | stat.S_IEXEC)
        print("✅ Created start_server.sh")
    except:
        print("⚠️ Created start_server.sh but couldn't make it executable")
    
    print("\n=== Setup Complete ===")
    print(f"Your Data Server is configured as a {args.server_id} server on port {args.port}")
    print(f"Main server URL: {args.main_url}")
    print("\nTo start the server:")
    print("- On Windows: run start_server.bat")
    print("- On Linux/Mac: run ./start_server.sh")
    print("\nMake sure the main bot server is running as well!")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 