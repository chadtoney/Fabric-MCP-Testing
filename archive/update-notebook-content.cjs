const { spawn } = require('child_process');
const fs = require('fs');

const config = {
    workspaceId: "bcb44215-0e69-46d3-aac9-fb92fadcd982",
    newNotebookId: "46c72ddd-5476-44b9-9a4d-cc859faaf326" // The newly created notebook
};

async function getAzureToken() {
    const { execSync } = require('child_process');
    try {
        const result = execSync('az account get-access-token --resource "https://analysis.windows.net/powerbi/api"', { encoding: 'utf8' });
        const tokenData = JSON.parse(result);
        return tokenData.accessToken;
    } catch (error) {
        console.error('Failed to get Azure token:', error.message);
        return null;
    }
}

async function sendMCPRequest(tool, params, timeout = 120000) {
    return new Promise((resolve, reject) => {
        const timeoutHandle = setTimeout(() => {
            reject(new Error(`Request timeout for ${tool}`));
        }, timeout);

        const requestId = Date.now();
        const request = {
            jsonrpc: "2.0",
            id: requestId,
            method: "tools/call",
            params: {
                name: tool,
                arguments: params
            }
        };

        console.log(`📤 ${tool}...`);

        let responseReceived = false;

        const dataHandler = (data) => {
            if (responseReceived) return;
            
            const output = data.toString();
            
            try {
                const lines = output.split('\n');
                for (const line of lines) {
                    if (line.trim()) {
                        const parsed = JSON.parse(line);
                        if (parsed.id === requestId) {
                            responseReceived = true;
                            clearTimeout(timeoutHandle);
                            
                            if (parsed.error) {
                                reject(new Error(`MCP error: ${parsed.error.message}`));
                            } else {
                                resolve(parsed.result);
                            }
                            return;
                        }
                    }
                }
            } catch (e) {
                // Continue collecting
            }
        };

        mcpProcess.stdout.on('data', dataHandler);
        mcpProcess.stderr.on('data', dataHandler);

        mcpProcess.stdin.write(JSON.stringify(request) + '\n');
    });
}

function convertVSCodeNotebookToJupyter(vscodeContent) {
    const cells = [];
    
    // More robust parsing - look for cell boundaries
    const lines = vscodeContent.split('\n');
    let currentCell = null;
    let currentContent = [];
    
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        
        if (line.includes('<VSCode.Cell')) {
            // Save previous cell if exists
            if (currentCell && currentContent.length > 0) {
                const content = currentContent.join('\n').trim();
                if (content) {
                    if (currentCell.language === 'markdown') {
                        cells.push({
                            cell_type: 'markdown',
                            metadata: {},
                            source: [content]
                        });
                    } else if (currentCell.language === 'python') {
                        cells.push({
                            cell_type: 'code',
                            execution_count: null,
                            metadata: {},
                            outputs: [],
                            source: [content]
                        });
                    }
                }
            }
            
            // Start new cell
            const langMatch = line.match(/language="([^"]+)"/);
            currentCell = {
                language: langMatch ? langMatch[1] : 'python'
            };
            currentContent = [];
        } else if (line.includes('</VSCode.Cell>')) {
            // End of cell - will be processed on next cell start or end of file
            continue;
        } else if (currentCell) {
            // Content line
            currentContent.push(line);
        }
    }
    
    // Handle last cell
    if (currentCell && currentContent.length > 0) {
        const content = currentContent.join('\n').trim();
        if (content) {
            if (currentCell.language === 'markdown') {
                cells.push({
                    cell_type: 'markdown',
                    metadata: {},
                    source: [content]
                });
            } else if (currentCell.language === 'python') {
                cells.push({
                    cell_type: 'code',
                    execution_count: null,
                    metadata: {},
                    outputs: [],
                    source: [content]
                });
            }
        }
    }
    
    return {
        cells: cells,
        metadata: {
            kernelspec: {
                display_name: "Python 3",
                language: "python",
                name: "python3"
            },
            language_info: {
                name: "python",
                version: "3.8.0"
            }
        },
        nbformat: 4,
        nbformat_minor: 4
    };
}

let mcpProcess;

async function updateNotebookContent() {
    console.log('📝 Updating New Notebook with Full Content');
    console.log('='.repeat(45));
    
    const token = await getAzureToken();
    if (!token) {
        console.error('❌ Failed to get Azure token');
        return;
    }
    
    console.log(`✅ Got Azure token`);
    
    // Read the local notebook
    console.log('📖 Reading local notebook...');
    const notebookPath = 'fabric-lakehouse-data-population.ipynb';
    const vscodeContent = fs.readFileSync(notebookPath, 'utf8');
    console.log(`✅ Read notebook (${vscodeContent.length} characters)`);
    
    // Convert to standard Jupyter format with improved parsing
    console.log('🔄 Converting to Jupyter format...');
    const jupyterNotebook = convertVSCodeNotebookToJupyter(vscodeContent);
    console.log(`✅ Converted to Jupyter format (${jupyterNotebook.cells.length} cells)`);
    
    // Show what we extracted
    console.log('\n📋 Extracted Cells:');
    jupyterNotebook.cells.forEach((cell, index) => {
        const preview = cell.source[0].substring(0, 50) + (cell.source[0].length > 50 ? '...' : '');
        console.log(`  ${index + 1}. ${cell.cell_type}: ${preview}`);
    });
    
    if (jupyterNotebook.cells.length === 0) {
        console.error('❌ No cells extracted - conversion failed');
        return;
    }
    
    const jupyterContent = JSON.stringify(jupyterNotebook, null, 2);
    const base64Content = Buffer.from(jupyterContent).toString('base64');
    
    mcpProcess = spawn('node', ['C:\\Repos\\Fabric-Analytics-MCP\\build\\index.js'], {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: 'C:\\Repos\\Fabric-Analytics-MCP'
    });

    await new Promise(resolve => setTimeout(resolve, 3000));

    try {
        console.log('\n🚀 Updating notebook definition with content...');
        const updateResult = await sendMCPRequest('update-fabric-notebook-definition', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            notebookId: config.newNotebookId,
            definition: {
                format: "ipynb",
                parts: [
                    {
                        path: "notebook-content.ipynb",
                        payload: base64Content,
                        payloadType: "InlineBase64"
                    }
                ]
            }
        });
        
        console.log('\n📋 Update Result:');
        console.log('='.repeat(40));
        console.log(updateResult.content[0].text);
        console.log('='.repeat(40));
        
        if (updateResult.content[0].text.includes('success') || updateResult.content[0].text.includes('updated')) {
            console.log('\n🎉 SUCCESS! Notebook updated with full content!');
            console.log('✅ Your comprehensive data population notebook is now ready');
            console.log(`📝 Notebook ID: ${config.newNotebookId}`);
            console.log('📊 Contains all cells with complete data generation code');
            console.log('🚀 Ready to run in Fabric to populate your lakehouse');
        } else {
            console.log('\n✅ Update completed - check Fabric portal to verify content');
        }
        
    } catch (error) {
        console.error('❌ Error updating notebook:', error.message);
    }

    console.log('\n🛑 Cleaning up...');
    mcpProcess.kill();
}

updateNotebookContent().catch(console.error);
