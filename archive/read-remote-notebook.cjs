const { spawn } = require('child_process');

const config = {
    workspaceId: "bcb44215-0e69-46d3-aac9-fb92fadcd982",
    notebookId: "46c72ddd-5476-44b9-9a4d-cc859faaf326" // The newly created notebook
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

async function readRemoteNotebook() {
    console.log('📖 Reading Notebook from Fabric Workspace');
    console.log('='.repeat(45));
    
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
        console.log('\n🚀 Getting notebook metadata...');
        const notebookInfo = await sendMCPRequest('get-fabric-notebook', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            notebookId: config.notebookId
        });
        
        console.log('\n📋 Notebook Metadata:');
        console.log('='.repeat(30));
        console.log(notebookInfo.content[0].text);
        console.log('='.repeat(30));
        
        console.log('\n🚀 Getting notebook definition (content)...');
        const notebookDefinition = await sendMCPRequest('get-fabric-notebook-definition', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            notebookId: config.notebookId,
            format: 'ipynb'
        });
        
        console.log('\n📄 Notebook Content:');
        console.log('='.repeat(40));
        const content = notebookDefinition.content[0].text;
        console.log(content);
        console.log('='.repeat(40));
        
        // Try to parse and analyze the content
        try {
            if (content && content.trim() !== '' && !content.includes('No notebook definition found')) {
                // Try to parse as JSON to see the structure
                const notebookData = JSON.parse(content);
                console.log('\n📊 Content Analysis:');
                console.log(`  📝 Format: ${notebookData.nbformat ? 'Jupyter Notebook' : 'Unknown'}`);
                console.log(`  📱 Cells: ${notebookData.cells ? notebookData.cells.length : 'Unknown'}`);
                
                if (notebookData.cells && notebookData.cells.length > 0) {
                    console.log('\n📋 Cell Summary:');
                    notebookData.cells.forEach((cell, index) => {
                        const type = cell.cell_type || 'unknown';
                        const preview = cell.source ? 
                            (Array.isArray(cell.source) ? cell.source[0] : cell.source).substring(0, 60) + '...' :
                            'No content';
                        console.log(`  ${index + 1}. ${type}: ${preview}`);
                    });
                    
                    console.log('\n🎉 SUCCESS! Notebook contains full content!');
                    console.log('✅ The notebook was successfully uploaded with all cells');
                    console.log('📊 Ready to run in Fabric workspace');
                } else {
                    console.log('\n⚠️ Notebook structure found but no cells detected');
                }
            } else {
                console.log('\n⚠️ Notebook definition is empty or not found');
                console.log('💡 This might mean the content is stored differently or needs time to sync');
            }
        } catch (parseError) {
            console.log('\n📝 Raw content retrieved (not JSON format):');
            console.log('⚠️ Content might be in a different format or encoding');
        }
        
        console.log('\n🔍 Additional Check: Try getting as different format...');
        try {
            const altResult = await sendMCPRequest('get-fabric-notebook-definition', {
                bearerToken: token,
                workspaceId: config.workspaceId,
                notebookId: config.notebookId
                // No format specified
            });
            
            console.log('\n📄 Alternative format result:');
            console.log(altResult.content[0].text.substring(0, 500) + (altResult.content[0].text.length > 500 ? '...' : ''));
        } catch (altError) {
            console.log('⚠️ Alternative format also failed');
        }
        
    } catch (error) {
        console.error('❌ Error reading notebook:', error.message);
    }

    console.log('\n🛑 Cleaning up...');
    mcpProcess.kill();
}

readRemoteNotebook().catch(console.error);
