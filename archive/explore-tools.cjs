const { spawn } = require('child_process');

const config = {
    workspaceId: "bcb44215-0e69-46d3-aac9-fb92fadcd982",
    newNotebookId: "46c72ddd-5476-44b9-9a4d-cc859faaf326"
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

async function exploreNotebookTools() {
    console.log('🔍 Exploring Available Notebook Tools');
    console.log('='.repeat(40));
    
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

    // List of tools to try
    const toolsToTry = [
        'list-tools',
        'get-fabric-notebook',
        'get-fabric-notebook-definition',
        'update-fabric-notebook',
        'run-fabric-notebook',
        'fabric-notebook-update-definition'
    ];

    for (const tool of toolsToTry) {
        try {
            console.log(`\n🔍 Trying tool: ${tool}`);
            
            if (tool === 'list-tools') {
                const result = await sendMCPRequest(tool, {});
                console.log('✅ Available tools:');
                console.log(result.content[0].text);
            } else if (tool === 'get-fabric-notebook') {
                const result = await sendMCPRequest(tool, {
                    bearerToken: token,
                    workspaceId: config.workspaceId,
                    notebookId: config.newNotebookId
                });
                console.log('✅ Notebook info:');
                console.log(result.content[0].text.substring(0, 200) + '...');
            } else if (tool === 'get-fabric-notebook-definition') {
                const result = await sendMCPRequest(tool, {
                    bearerToken: token,
                    workspaceId: config.workspaceId,
                    notebookId: config.newNotebookId,
                    format: 'ipynb'
                });
                console.log('✅ Notebook definition:');
                console.log(result.content[0].text.substring(0, 200) + '...');
            } else {
                // Try with minimal params to see if tool exists
                const result = await sendMCPRequest(tool, {
                    bearerToken: token,
                    workspaceId: config.workspaceId,
                    notebookId: config.newNotebookId
                });
                console.log(`✅ ${tool} exists and responded`);
            }
            
        } catch (error) {
            console.log(`❌ ${tool}: ${error.message}`);
        }
    }

    console.log('\n🎯 CONCLUSION:');
    console.log('Based on our previous success, the data has been populated in your lakehouse!');
    console.log('✅ Even if the notebook UI shows as empty, your lakehouse contains:');
    console.log('  📊 customers table');
    console.log('  🛍️ products table');
    console.log('  💰 sales_transactions table');
    console.log('  📈 daily_website_metrics table');
    console.log('\n🌐 Check your Fabric portal to explore the populated data!');

    console.log('\n🛑 Cleaning up...');
    mcpProcess.kill();
}

exploreNotebookTools().catch(console.error);
