const { spawn } = require('child_process');

const config = {
    workspaceId: "bcb44215-0e69-46d3-aac9-fb92fadcd982",
    testNotebookId: "5df3da0c-92c6-4e2d-82e8-d16bcb8572f3" // Your manually created notebook
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

let mcpProcess;

async function analyzeManualNotebook() {
    console.log('🔍 Analyzing Manually Created Fabric Notebook');
    console.log('='.repeat(50));
    console.log(`📝 Notebook ID: ${config.testNotebookId}`);
    console.log('🎯 Goal: Understand Fabric notebook format');
    console.log('='.repeat(50));
    
    const token = await getAzureToken();
    if (!token) {
        console.error('❌ Failed to get Azure token');
        return;
    }
    
    console.log(`✅ Got Azure token`);
    
    mcpProcess = spawn('node', ['C:\\Repos\\Fabric-Analytics-MCP\\build\\index.js'], {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: 'C:\\Repos\\Fabric-Analytics-MCP'
    });

    await new Promise(resolve => setTimeout(resolve, 3000));

    try {
        console.log('\n📋 1. Getting notebook metadata...');
        const notebookInfo = await sendMCPRequest('get-fabric-notebook', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            notebookId: config.testNotebookId
        });
        
        console.log('\n📊 Notebook Metadata:');
        console.log('='.repeat(30));
        console.log(notebookInfo.content[0].text);
        console.log('='.repeat(30));
        
        console.log('\n📋 2. Getting notebook definition with format ipynb...');
        try {
            const notebookDef = await sendMCPRequest('get-fabric-notebook-definition', {
                bearerToken: token,
                workspaceId: config.workspaceId,
                notebookId: config.testNotebookId,
                format: 'ipynb'
            });
            
            const content = notebookDef.content[0].text;
            console.log('\n📄 Notebook Definition (ipynb format):');
            console.log('='.repeat(40));
            console.log(content);
            console.log('='.repeat(40));
            
            if (content && !content.includes('No notebook definition found')) {
                console.log('\n🎉 SUCCESS! Content found with ipynb format!');
                
                try {
                    const parsed = JSON.parse(content);
                    console.log('\n📊 Analysis:');
                    console.log(`  📝 Format: ${parsed.nbformat || 'Unknown'}`);
                    console.log(`  📱 Cells: ${parsed.cells ? parsed.cells.length : 'None'}`);
                    console.log(`  🔧 Metadata keys: ${Object.keys(parsed.metadata || {}).join(', ')}`);
                    
                    if (parsed.cells && parsed.cells.length > 0) {
                        console.log('\n📋 Cell Details:');
                        parsed.cells.forEach((cell, index) => {
                            console.log(`  Cell ${index + 1}:`);
                            console.log(`    Type: ${cell.cell_type}`);
                            console.log(`    Source length: ${cell.source ? (Array.isArray(cell.source) ? cell.source.length : 1) : 0} lines`);
                            if (cell.source) {
                                const preview = Array.isArray(cell.source) ? cell.source[0] : cell.source;
                                console.log(`    Preview: ${preview.substring(0, 60)}...`);
                            }
                        });
                    }
                    
                    // Save this as our template
                    const fs = require('fs');
                    fs.writeFileSync('fabric-notebook-template.json', JSON.stringify(parsed, null, 2));
                    console.log('\n💾 Saved notebook structure as template: fabric-notebook-template.json');
                    
                } catch (parseError) {
                    console.log('\n⚠️ Content found but could not parse as JSON');
                    console.log('Raw content preview:');
                    console.log(content.substring(0, 500) + '...');
                }
                
            } else {
                console.log('\n❌ No content found with ipynb format');
            }
            
        } catch (defError) {
            console.log(`\n❌ Definition error: ${defError.message}`);
        }
        
        console.log('\n📋 3. Trying without format specification...');
        try {
            const notebookDefAlt = await sendMCPRequest('get-fabric-notebook-definition', {
                bearerToken: token,
                workspaceId: config.workspaceId,
                notebookId: config.testNotebookId
            });
            
            const altContent = notebookDefAlt.content[0].text;
            console.log('\n📄 Alternative Format:');
            console.log('='.repeat(30));
            console.log(altContent.substring(0, 1000) + (altContent.length > 1000 ? '...' : ''));
            console.log('='.repeat(30));
            
        } catch (altError) {
            console.log(`\n❌ Alternative format error: ${altError.message}`);
        }
        
        console.log('\n🎯 CONCLUSION:');
        if (notebookInfo.content[0].text.includes('Name:')) {
            console.log('✅ Notebook exists and is accessible');
            console.log('🔍 Now we know the exact format Fabric uses!');
            console.log('📝 We can use this template to create proper notebooks');
        } else {
            console.log('⚠️ Unexpected response format');
        }
        
    } catch (error) {
        console.error('❌ Error analyzing notebook:', error.message);
    }

    console.log('\n🛑 Cleaning up...');
    mcpProcess.kill();
}

analyzeManualNotebook().catch(console.error);
