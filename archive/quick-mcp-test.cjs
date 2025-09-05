const { spawn } = require('child_process');

const config = {
    workspaceId: "bcb44215-0e69-46d3-aac9-fb92fadcd982",
    lakehouseId: "5e6b33fe-1f33-419a-a954-bce697ccfe61"
};

let mcpProcess;

async function sendMCPRequest(tool, params, timeout = 30000) {
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

        console.log(`📤 Sending: ${tool}`);

        let responseReceived = false;

        const dataHandler = (data) => {
            if (responseReceived) return;
            
            const output = data.toString();
            console.log('📥 Raw output:', output);
            
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

async function quickTest() {
    console.log('🧪 Quick MCP Test');
    
    mcpProcess = spawn('node', ['C:\\Repos\\Fabric-Analytics-MCP\\build\\index.js'], {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: 'C:\\Repos\\Fabric-Analytics-MCP'
    });

    // Let's capture all output for debugging
    mcpProcess.stdout.on('data', (data) => {
        console.log('STDOUT:', data.toString());
    });

    mcpProcess.stderr.on('data', (data) => {
        console.log('STDERR:', data.toString());
    });

    // Wait for startup
    await new Promise(resolve => setTimeout(resolve, 3000));

    try {
        // Simple capability check first
        console.log('\n📋 Testing Livy session creation...');
        const sessionResult = await sendMCPRequest('create-livy-session', {
            bearerToken: 'azure_cli',
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionConfig: {
                kind: 'pyspark'
            }
        });
        
        console.log('✅ Session creation result:', JSON.stringify(sessionResult, null, 2));
        
    } catch (error) {
        console.error('❌ Error:', error.message);
    }

    console.log('\n🛑 Stopping MCP server...');
    mcpProcess.kill();
}

quickTest().catch(console.error);
