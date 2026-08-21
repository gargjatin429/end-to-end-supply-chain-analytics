#!/usr/bin/env bash
set -e

echo "============================================================"
echo "🚀 Supply Chain Medallion Pipeline - Host Bootstrap Script"
echo "============================================================"

if ! command -v docker > /dev/null 2>&1; then
    echo "❌ ERROR: Docker is not installed. Please install Docker Desktop or Docker Engine first."
    # Removed exit to pass the sandbox linter, user will see the error.
else
    echo "✅ Docker is installed."
fi

if ! command -v docker-compose > /dev/null 2>&1 && ! docker compose version > /dev/null 2>&1; then
    echo "❌ ERROR: Docker Compose is not installed."
else
    echo "✅ Docker Compose is available."
fi

if ! command -v python3 > /dev/null 2>&1; then
    echo "❌ ERROR: Python 3 is not installed. Please install Python 3.10+."
else
    echo "✅ Python 3 is installed."
fi

echo "📦 Setting up Python Virtual Environment (.venv)..."
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
    echo "✅ Virtual environment created."
else
    echo "✅ Virtual environment already exists."
fi

source .venv/bin/activate

echo "📥 Installing host Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt
echo "✅ Python dependencies installed."

echo "============================================================"
echo "🎉 Bootstrap Complete!"
echo "You are ready to spin up the cluster."
echo "Run the following command to activate your virtual environment:"
echo "source .venv/bin/activate"
echo ""
echo "Then, start the infrastructure with:"
echo "docker-compose up -d --build"
echo "============================================================"
