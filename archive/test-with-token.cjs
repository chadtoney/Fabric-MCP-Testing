const { spawn } = require('child_process');

const config = {
    workspaceId: "bcb44215-0e69-46d3-aac9-fb92fadcd982",
    lakehouseId: "5e6b33fe-1f33-419a-a954-bce697ccfe61"
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

async function sendMCPRequest(tool, params, timeout = 60000) {
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

async function testWithToken() {
    console.log('🧪 Test with Real Token');
    
    // Get real Azure token
    const token = await getAzureToken();
    if (!token) {
        console.error('❌ Failed to get Azure token');
        return;
    }
    
    console.log(`✅ Got token (length: ${token.length})`);
    
    mcpProcess = spawn('node', ['C:\\Repos\\Fabric-Analytics-MCP\\build\\index.js'], {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: 'C:\\Repos\\Fabric-Analytics-MCP'
    });

    // Wait for startup
    await new Promise(resolve => setTimeout(resolve, 3000));

    try {
        console.log('\n📋 Creating Livy session...');
        const sessionResult = await sendMCPRequest('create-livy-session', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionConfig: {
                kind: 'pyspark'
            }
        });
        
        console.log('✅ Session creation result:', JSON.stringify(sessionResult, null, 2));
        
        // If session creation succeeded, extract session ID and try a simple statement
        const responseText = sessionResult.content[0].text;
        if (responseText.includes('"id"')) {
            const sessionData = JSON.parse(responseText);
            const sessionId = sessionData.id;
            console.log(`\n📋 Got session ID: ${sessionId}`);
            
            console.log('\n📋 Executing simple statement...');
            const statementResult = await sendMCPRequest('execute-livy-statement', {
                bearerToken: token,
                workspaceId: config.workspaceId,
                lakehouseId: config.lakehouseId,
                sessionId: sessionId,
                code: 'print("Hello from Fabric MCP!")',
                kind: 'pyspark'
            });
            
            console.log('✅ Statement execution result:', JSON.stringify(statementResult, null, 2));
        }
        
    } catch (error) {
        console.error('❌ Error:', error.message);
    }

    console.log('\n🛑 Stopping MCP server...');
    mcpProcess.kill();
}

testWithToken().catch(console.error);
