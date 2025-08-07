#!/usr/bin/env python3
"""
Java Version Checker for Spark 4.0 Compatibility
"""

import subprocess
import sys
import re
from typing import Optional, Tuple


def get_java_version() -> Optional[Tuple[int, int]]:
    """Get Java version as (major, minor) tuple."""
    try:
        result = subprocess.run(['java', '-version'], 
                              capture_output=True, text=True)
        
        # Parse version from output like: "openjdk version "17.0.9" 2023-10-17"
        # Java version output goes to stderr
        version_match = re.search(r'version "(\d+)\.(\d+)', result.stderr)
        if version_match:
            major = int(version_match.group(1))
            minor = int(version_match.group(2))
            return (major, minor)
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    return None


def check_spark4_compatibility() -> bool:
    """Check if current Java version is compatible with Spark 4.0."""
    version = get_java_version()
    
    if version is None:
        print("❌ Java not found or not accessible")
        return False
    
    major, minor = version
    print(f"🔍 Found Java version: {major}.{minor}")
    
    # Spark 4.0 requires Java 17+ (JDK 8/11 are dropped)
    if major >= 17:
        print(f"✅ Java {major}.{minor} is compatible with Spark 4.0")
        if major == 17:
            print("   📌 Java 17 is the recommended default for Spark 4.0")
        elif major == 21:
            print("   📌 Java 21 is also fully supported")
        else:
            print(f"   📌 Java {major} is supported")
        return True
    else:
        print(f"❌ Java {major}.{minor} is NOT compatible with Spark 4.0")
        print("   📋 Spark 4.0 requires Java 17 or 21")
        print("   📋 Java 8 and 11 are no longer supported")
        return False


def main():
    """Main function to check Java compatibility."""
    print("🚀 Spark 4.0 Java Version Compatibility Check")
    print("=" * 50)
    
    compatible = check_spark4_compatibility()
    
    print("\n" + "=" * 50)
    if compatible:
        print("✅ Your Java version is ready for Spark 4.0!")
    else:
        print("❌ Please upgrade to Java 17 or 21 for Spark 4.0")
        print("\n📥 Installation options:")
        print("   • Docker: Use the provided Dockerfile (includes Java 17)")
        print("   • Ubuntu/Debian: sudo apt-get install openjdk-17-jdk")
        print("   • macOS: brew install openjdk@17")
        print("   • Windows: Download from https://adoptium.net/")
    
    return 0 if compatible else 1


if __name__ == "__main__":
    sys.exit(main())
