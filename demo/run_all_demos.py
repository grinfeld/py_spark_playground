#!/usr/bin/env python3
"""
Run all demo scripts in the demo directory.

This script executes all demo files to show the complete functionality
of the configuration management system.
"""

import os
import sys
import subprocess
import glob
from pathlib import Path

def run_all_demos():
    """Run all demo scripts in the current directory."""
    
    # Get the current directory (demo folder)
    demo_dir = Path(__file__).parent
    
    # Find all Python demo files
    demo_files = glob.glob(str(demo_dir / "demo_*.py"))
    demo_files.sort()  # Sort for consistent order
    
    print("🚀 Running all demo scripts...")
    print("=" * 60)
    
    for demo_file in demo_files:
        demo_name = Path(demo_file).stem
        print(f"\n📁 Running: {demo_name}")
        print("-" * 40)
        
        try:
            # Run the demo script
            result = subprocess.run([
                sys.executable, demo_file
            ], capture_output=True, text=True, cwd=demo_dir.parent)
            
            if result.returncode == 0:
                print("✅ Success")
                if result.stdout:
                    print(result.stdout)
            else:
                print("❌ Failed")
                if result.stderr:
                    print(result.stderr)
                    
        except Exception as e:
            print(f"❌ Error running {demo_name}: {e}")
    
    print("\n" + "=" * 60)
    print("🎉 All demos completed!")


def list_demos():
    """List all available demo scripts."""
    
    demo_dir = Path(__file__).parent
    demo_files = glob.glob(str(demo_dir / "demo_*.py"))
    demo_files.sort()
    
    print("📁 Available demo scripts:")
    print("=" * 40)
    
    for demo_file in demo_files:
        demo_name = Path(demo_file).stem
        print(f"  • {demo_name}")
    
    print(f"\nTotal: {len(demo_files)} demo scripts")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--list":
        list_demos()
    else:
        run_all_demos()
