# Setup Guide - Microsoft Fabric MCP Server Examples

## 📋 Prerequisites

### 1. Microsoft Fabric MCP Server

**This repository contains examples for the Fabric MCP Server. You must install the server first.**

#### Install the Main MCP Server:
```bash
# Clone the main MCP server repository
git clone https://github.com/santhoshravindran7/Fabric-Analytics-MCP.git
cd Fabric-Analytics-MCP

# Install dependencies
npm install

# Build the server
npm run build
```

#### Alternative: Download Release
Visit the [releases page](https://github.com/santhoshravindran7/Fabric-Analytics-MCP/releases) for pre-built versions.

### 2. Azure Authentication

```bash
# Install Azure CLI if not already installed
# Windows: winget install Microsoft.AzureCLI
# Mac: brew install azure-cli
# Linux: curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Authenticate with Azure
az login
```

### 3. Microsoft Fabric Access

- ✅ Access to a Microsoft Fabric workspace
- ✅ Permissions to create/modify items in the workspace
- ✅ Know your workspace ID (found in Fabric portal URL)

## ⚙️ Configuration

### Environment Variables

Create these environment variables for your system:

#### Windows (PowerShell):
```powershell
# Required
$env:FABRIC_AUTH_METHOD = "azure_cli"
$env:FABRIC_DEFAULT_WORKSPACE_ID = "your-workspace-id-here"

# Optional - Path to MCP server if not in default location
$env:FABRIC_MCP_SERVER_PATH = "C:\Path\To\Fabric-Analytics-MCP"
```

#### Linux/Mac (Bash):
```bash
# Add to your .bashrc or .zshrc
export FABRIC_AUTH_METHOD="azure_cli" 
export FABRIC_DEFAULT_WORKSPACE_ID="your-workspace-id-here"

# Optional - Path to MCP server
export FABRIC_MCP_SERVER_PATH="/path/to/Fabric-Analytics-MCP"
```

### Find Your Workspace ID

1. **Go to Microsoft Fabric Portal**: https://app.fabric.microsoft.com/
2. **Navigate to your workspace**
3. **Copy the ID from the URL**:
   ```
   https://app.fabric.microsoft.com/groups/12345678-1234-1234-1234-123456789abc
                                           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                                           This is your workspace ID
   ```

## 🚀 Quick Start

### 1. Test Your Setup

Run the basic demo to verify everything works:

```bash
cd examples/getting-started/
node basic-demo.cjs
```

**Expected output:**
```
🚀 Microsoft Fabric MCP Server - Getting Started Demo
============================================================
🔧 Starting MCP server...
📋 Step 1: Check available tools...
✅ Found 62 MCP tools available
   📁 Workspace: 5 tools
   📁 Lakehouse: 8 tools
   📁 Notebook: 12 tools
   📁 Spark: 15 tools
🏢 Step 2: Check workspace access...
✅ Workspace access confirmed
```

### 2. Try Data Population

```bash
cd examples/data-population/
node execute-notebook-livy-corrected.cjs
```

This will populate your lakehouse with 5,000+ sample records.

### 3. Troubleshoot Issues

```bash
cd examples/troubleshooting/
node test-capacity-issue.cjs
node investigate-mcp-notebook-tools.cjs
```

## 🔧 Common Issues

### Issue 1: "Cannot find module"
**Problem**: MCP server path not found
**Solution**: Set `FABRIC_MCP_SERVER_PATH` environment variable

### Issue 2: "Authentication failed"
**Problem**: Azure CLI not authenticated
**Solution**: Run `az login` and ensure you have Fabric access

### Issue 3: "Workspace not found"
**Problem**: Invalid workspace ID
**Solution**: Verify workspace ID from Fabric portal URL

### Issue 4: "Please update config.workspaceId"
**Problem**: Using placeholder workspace ID
**Solution**: Set `FABRIC_DEFAULT_WORKSPACE_ID` environment variable

## 📚 Repository Structure

```
📦 This Repository (Examples)
├── examples/getting-started/     # Start here
├── examples/data-population/     # Production workflows
├── examples/troubleshooting/     # Debug tools
└── docs/                         # Additional guides

📦 Main MCP Server Repository
├── src/                          # Server source code
├── build/                        # Compiled server (run npm run build)
└── README.md                     # Server documentation
```

## 🔗 Related Links

- **Main MCP Server**: https://github.com/santhoshravindran7/Fabric-Analytics-MCP
- **Pull Request #9**: https://github.com/santhoshravindran7/Fabric-Analytics-MCP/pull/9 (UUID session ID fix)
- **Issue #10**: https://github.com/santhoshravindran7/Fabric-Analytics-MCP/issues/10 (Notebook content limitation)
- **Issue #12**: https://github.com/santhoshravindran7/Fabric-Analytics-MCP/issues/12 (Missing capacity tools)
- **Model Context Protocol**: https://modelcontextprotocol.io/
- **Microsoft Fabric**: https://docs.microsoft.com/en-us/fabric/

## ❓ Getting Help

1. **Check troubleshooting examples** in `examples/troubleshooting/`
2. **Review issues** in the main repository
3. **Test with minimal configuration** using `basic-demo.cjs`
4. **Open issues** in the main repository with reproduction steps

---

**💡 Remember**: This repository contains examples and tests. The actual MCP server is in the [Fabric-Analytics-MCP repository](https://github.com/santhoshravindran7/Fabric-Analytics-MCP).
