# Microsoft Fabric MCP Server - Examples & Testing

> **🔗 This repository contains examples for the [Microsoft Fabric MCP Server](https://github.com/santhoshravindran7/Fabric-Analytics-MCP)**

Real-world examples demonstrating how to use the Microsoft Fabric Model Context Protocol (MCP) server to automate Fabric operations including lakehouse management, notebook execution, and data population workflows.

## 🚀 Quick Start

### 1. Install the MCP Server First

**⚠️ Important**: You need the main MCP server installed before using these examples.

```bash
git clone https://github.com/santhoshravindran7/Fabric-Analytics-MCP.git
cd Fabric-Analytics-MCP
npm install && npm run build
```

### 2. Set Your Configuration

```bash
# Set your workspace ID (required)
export FABRIC_DEFAULT_WORKSPACE_ID="your-workspace-id-from-fabric-portal"

# Authenticate with Azure
az login
```

### 3. Run Your First Example

```bash
git clone https://github.com/your-username/Fabric-MCP-Testing.git
cd Fabric-MCP-Testing/examples/getting-started
node basic-demo.cjs
```

Expected result: ✅ Connection verified with 62 available MCP tools

## 📁 Repository Structure

### 🌟 Examples (Start Here)

| Directory | Purpose | Key Files |
|-----------|---------|-----------|
| **`getting-started/`** | New user introduction | `basic-demo.cjs` - Test your setup |
| **`data-population/`** | Production workflows | `execute-notebook-livy-corrected.cjs` - 5K+ records |
| **`troubleshooting/`** | Debug & investigation | `test-capacity-issue.cjs` - Reproduce bugs |

### 📚 Documentation

| File | Description |
|------|-------------|
| `SETUP.md` | Complete setup instructions |
| `docs/DEMO_SUCCESS_SUMMARY.md` | Detailed success stories and workflows |

### 🗂️ Development

| Directory | Purpose |
|-----------|---------|
| `notebooks/` | Jupyter notebooks for testing |
| `archive/` | Historical scripts and experiments |

## 🔧 What These Examples Do

### Basic Operations
- ✅ **MCP Server Connection**: Establish and test connection
- ✅ **Workspace Access**: Verify permissions and list items
- ✅ **Tool Discovery**: Find all 62 available MCP functions

### Production Workflows
- 🏭 **Data Population**: Generate 5,000+ structured records
- 📊 **Lakehouse Creation**: Full lakehouse with tables and data
- 📈 **Notebook Execution**: Automated Spark job processing
- 🔍 **Session Management**: Livy API integration and monitoring

### Debugging & Troubleshooting
- 🐛 **Issue Reproduction**: Test cases for known issues
- 🔍 **Investigation Scripts**: Deep-dive into MCP behavior
- ⚠️ **Error Handling**: Robust error capture and reporting

## 🛠️ Technology Stack

- **MCP Server**: [Fabric-Analytics-MCP](https://github.com/santhoshravindran7/Fabric-Analytics-MCP)
- **Authentication**: Azure CLI (`az login`)
- **Platform**: Microsoft Fabric / Power BI Premium
- **APIs**: Microsoft Fabric REST API + Livy Spark API
- **Language**: Node.js (CommonJS modules)

## 📊 Real Results

### Data Population Success
```
📈 Results Summary:
├── 🏭 Lakehouse: Created with 3 tables
├── 📊 Records: 5,000+ structured entries
├── ⚡ Performance: Sub-minute execution
└── 🔄 Repeatability: 100% success rate
```

### MCP Integration
```
🔗 MCP Tools Available: 62 functions
├── 📁 Workspace Management: 5 tools
├── 🏠 Lakehouse Operations: 8 tools  
├── 📔 Notebook Control: 12 tools
├── ⚡ Spark Job Management: 15 tools
└── 🔧 Utility Functions: 22 tools
```

## 🎯 Use Cases

### For Developers
- **Learn MCP Integration**: Real working examples
- **Test Your Setup**: Validation scripts included
- **Production Patterns**: Battle-tested workflows

### For Data Engineers  
- **Automate Fabric**: Lakehouse creation and data loading
- **Spark Workflows**: Notebook execution and monitoring
- **ETL Pipelines**: End-to-end data processing examples

### For DevOps Teams
- **CI/CD Integration**: Automated Fabric deployments
- **Monitoring**: Error handling and logging patterns
- **Troubleshooting**: Debug scripts for common issues

## 🔗 Related Projects

### Main Dependencies
- **[Fabric-Analytics-MCP](https://github.com/santhoshravindran7/Fabric-Analytics-MCP)** - The MCP server (required)
- **[Model Context Protocol](https://modelcontextprotocol.io/)** - MCP specification
- **[Microsoft Fabric](https://docs.microsoft.com/en-us/fabric/)** - Platform documentation

### Known Issues & Fixes
- **[Issue #12](https://github.com/santhoshravindran7/Fabric-Analytics-MCP/issues/12)** - Missing capacity tools
- **[PR #9](https://github.com/santhoshravindran7/Fabric-Analytics-MCP/pull/9)** - UUID session ID fix
- **[Issue #10](https://github.com/santhoshravindran7/Fabric-Analytics-MCP/issues/10)** - Notebook content limitation

## 🤝 Contributing

1. **Test scenarios** welcome in `examples/troubleshooting/`
2. **Production workflows** needed in `examples/data-population/`
3. **Documentation improvements** always appreciated
4. **Bug reproductions** help the main project

## 📄 License

MIT License - Feel free to use these examples in your own projects.

## 🆘 Getting Help

1. **Start with**: `examples/getting-started/basic-demo.cjs`
2. **Check**: `SETUP.md` for detailed instructions
3. **Debug with**: Scripts in `examples/troubleshooting/`
4. **Ask questions**: In the main [Fabric-Analytics-MCP repository](https://github.com/santhoshravindran7/Fabric-Analytics-MCP/issues)

---

**⭐ Star this repo** if these examples helped you get started with Microsoft Fabric MCP automation!
