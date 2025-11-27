#!/bin/bash
# FunnelPulse Setup Script
# ========================
# This script sets up the local development environment

set -e

echo "=========================================="
echo "FunnelPulse - Local Setup"
echo "=========================================="

# Check Python version
echo ""
echo "Checking Python version..."
python3 --version || { echo "Python 3 is required. Please install Python 3.8+"; exit 1; }

# Create virtual environment
echo ""
echo "Creating virtual environment..."
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "Virtual environment created."
else
    echo "Virtual environment already exists."
fi

# Activate virtual environment
echo ""
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo ""
echo "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo ""
echo "Installing dependencies from requirements.txt..."
pip install -r requirements.txt

# Create necessary directories
echo ""
echo "Creating project directories..."
mkdir -p data_raw tables stream_input checkpoints

# Check Java (required for PySpark)
echo ""
echo "Checking Java installation..."
if command -v java &> /dev/null; then
    java -version 2>&1 | head -n 1
    echo "Java is installed."
else
    echo "WARNING: Java is not installed. PySpark requires Java 8, 11, or 17."
    echo "Please install Java before running Spark jobs."
    echo ""
    echo "On macOS: brew install openjdk@17"
    echo "On Ubuntu: sudo apt install openjdk-17-jdk"
fi

echo ""
echo "=========================================="
echo "Setup complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Activate the virtual environment:"
echo "   source venv/bin/activate"
echo ""
echo "2. Download the Kaggle dataset:"
echo "   - Option A (with Kaggle CLI):"
echo "     kaggle datasets download -d mkechinov/ecommerce-events-history-in-cosmetics-shop"
echo "     unzip ecommerce-events-history-in-cosmetics-shop.zip -d data_raw/"
echo ""
echo "   - Option B (manual download):"
echo "     Visit: https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop"
echo "     Download and extract CSV files to data_raw/"
echo ""
echo "3. Verify configuration:"
echo "   python config.py"
echo ""
echo "4. Start Jupyter Lab:"
echo "   jupyter lab"
echo ""
echo "5. Run notebooks in order (01 -> 06)"
echo "=========================================="
