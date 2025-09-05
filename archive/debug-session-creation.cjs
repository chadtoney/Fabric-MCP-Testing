const { spawn } = require('child_process');

const config = {
    workspaceId: "bcb44215-0e69-46d3-aac9-fb92fadcd982",
    lakehouseId: "5e6b33fe-1f33-419a-a954-bce697ccfe61"
};

let mcpProcess;

async function sendMCPRequest(tool, params) {
    return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
            reject(new Error('Request timeout'));
        }, 60000);

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

        console.log(`📤 Sending MCP request: ${tool}`);
        console.log(`📋 Parameters:`, JSON.stringify(params, null, 2));

        let responseBuffer = '';
        let responseReceived = false;

        const dataHandler = (data) => {
            responseBuffer += data.toString();
            
            try {
                const lines = responseBuffer.split('\n');
                for (const line of lines) {
                    if (line.trim()) {
                        const parsed = JSON.parse(line);
                        if (parsed.id === requestId && !responseReceived) {
                            responseReceived = true;
                            clearTimeout(timeout);
                            mcpProcess.stdout.off('data', dataHandler);
                            mcpProcess.stderr.off('data', dataHandler);
                            
                            if (parsed.error) {
                                reject(new Error(`MCP error ${parsed.error.code}: ${parsed.error.message}`));
                            } else {
                                resolve(parsed.result);
                            }
                        }
                    }
                }
            } catch (e) {
                // Continue collecting data
            }
        };

        mcpProcess.stdout.on('data', dataHandler);
        mcpProcess.stderr.on('data', dataHandler);

        mcpProcess.stdin.write(JSON.stringify(request) + '\n');
    });
}

async function debugSessionCreation() {
    console.log('🐛 Debugging Session Creation');
    console.log(`Workspace: ${config.workspaceId}`);
    console.log(`Lakehouse: ${config.lakehouseId}`);

    console.log('🔧 MCP Server: Microsoft Fabric Analytics MCP Server running on stdio');
    mcpProcess = spawn('node', ['build/index.js'], {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: process.cwd()
    });

    // Wait for server startup
    await new Promise(resolve => setTimeout(resolve, 3000));
    console.log('✅ MCP Server started successfully\n');

    try {
        // First, let's see what workspaces we have access to
        console.log('🔍 Step 1: Listing workspaces...');
        const workspaces = await sendMCPRequest('list-fabric-workspaces', {});
        console.log('✅ Workspaces found:', workspaces.length);
        
        // Find our workspace
        const workspace = workspaces.find(w => w.id === config.workspaceId);
        if (workspace) {
            console.log(`✅ Target workspace found: ${workspace.displayName}`);
        } else {
            console.log('❌ Target workspace not found!');
            return;
        }

        // Let's try to list Livy sessions first
        console.log('\n🔍 Step 2: Listing existing Livy sessions...');
        const sessions = await sendMCPRequest('list-livy-sessions', {
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId
        });
        console.log('✅ Current sessions:', sessions.length);
        sessions.forEach(session => {
            console.log(`  - Session ${session.id}: ${session.state} (${session.kind})`);
        });

        // Now try to create a new session with detailed logging
        console.log('\n🔄 Step 3: Creating new Livy session...');
        const sessionParams = {
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            name: "fresh-debug-session",
            executorCores: 4,
            executorMemory: "7g",
            driverCores: 4,
            driverMemory: "14g",
            timeout: 30
        };
        
        console.log('📋 Session parameters:', JSON.stringify(sessionParams, null, 2));
        
        const newSession = await sendMCPRequest('create-livy-session', sessionParams);
        console.log('📊 Raw session response:', JSON.stringify(newSession, null, 2));
        
        if (newSession && typeof newSession.id === 'number') {
            console.log(`✅ New session created with ID: ${newSession.id}`);
        } else {
            console.log('❌ Session creation failed - invalid response');
        }

    } catch (error) {
        console.error('❌ Error during debugging:', error.message);
    }

    console.log('\n🛑 Stopping MCP server...');
    mcpProcess.kill();
}

debugSessionCreation().catch(console.error);
