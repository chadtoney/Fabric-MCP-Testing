const { Client } = require('@modelcontextprotocol/sdk/client/index.js');
const { StdioClientTransport } = require('@modelcontextprotocol/sdk/client/stdio.js');

const config = {
    workspaceId: "bcb44215-0e69-46d3-aac9-fb92fadcd982",
    notebookId: "1df1becd-d296-4212-9e20-dceb390d3994",
    serverCommand: "node",
    serverArgs: ["build/index.js"]
};

async function sendMCPRequest(toolName, args) {
    return new Promise((resolve, reject) => {
        let serverOutput = '';
        let client;
        let transport;

        try {
            // Create transport and client
            transport = new StdioClientTransport({
                command: config.serverCommand,
                args: config.serverArgs,
                stderr: 'pipe'
            });

            client = new Client({
                name: "fabric-notebook-runner",
                version: "1.0.0"
            }, {
                capabilities: {
                    tools: {}
                }
            });

            // Handle server stderr for debugging
            if (transport.process && transport.process.stderr) {
                transport.process.stderr.on('data', (data) => {
                    serverOutput += data.toString();
                    if (data.toString().includes('Microsoft Fabric Analytics MCP Server running')) {
                        console.log('✅ MCP Server is ready');
                    }
                });
            }

            // Connect and make request
            client.connect(transport).then(async () => {
                console.log('🔗 Connected to MCP server');
                
                try {
                    const response = await client.request(
                        { method: 'tools/call', params: { name: toolName, arguments: args } },
                        { timeout: 60000 }
                    );
                    resolve(response);
                } catch (error) {
                    reject(error);
                } finally {
                    await client.close();
                }
            }).catch(reject);

        } catch (error) {
            reject(error);
        }
    });
}

async function runNotebookInFabric() {
    console.log('🚀 Running notebook in Fabric workspace using MCP...');
    console.log(`Workspace: ${config.workspaceId}`);
    console.log(`Notebook: ${config.notebookId}`);
    console.log('');

    try {
        console.log('📝 Attempting to run notebook using run-fabric-notebook...');
        
        const runResult = await sendMCPRequest('run-fabric-notebook', {
            workspaceId: config.workspaceId,
            notebookId: config.notebookId,
            parameters: {},
            configuration: {
                timeout: 1800 // 30 minutes
            }
        });

        console.log('✅ Notebook execution started!');
        console.log('Response:', JSON.stringify(runResult, null, 2));

        if (runResult.result && runResult.result.content) {
            const responseText = runResult.result.content[0].text;
            console.log('📋 Execution details:');
            console.log(responseText);

            // Extract job ID if available
            const jobIdMatch = responseText.match(/Job ID: ([a-f0-9-]+)/);
            if (jobIdMatch) {
                const jobId = jobIdMatch[1];
                console.log(`\n🔍 Monitor execution at:`);
                console.log(`https://app.fabric.microsoft.com/groups/${config.workspaceId}/`);
                console.log(`Job ID: ${jobId}`);
            }
        }

    } catch (runError) {
        console.log('⚠️ run-fabric-notebook failed, trying execute-fabric-notebook...');
        console.log('Error:', runError.message);

        try {
            const executeResult = await sendMCPRequest('execute-fabric-notebook', {
                workspaceId: config.workspaceId,
                notebookId: config.notebookId,
                parameters: {},
                timeout: 1800
            });

            console.log('✅ Notebook execution started with execute method!');
            console.log('Response:', JSON.stringify(executeResult, null, 2));

            if (executeResult.result && executeResult.result.content) {
                const responseText = executeResult.result.content[0].text;
                console.log('📋 Execution details:');
                console.log(responseText);
            }

        } catch (executeError) {
            console.log('❌ Both execution methods failed');
            console.log('run-fabric-notebook error:', runError.message);
            console.log('execute-fabric-notebook error:', executeError.message);
            
            console.log('');
            console.log('🛠️ Manual execution required:');
            console.log('1. Open: https://app.fabric.microsoft.com/');
            console.log(`2. Navigate to workspace: ${config.workspaceId}`);
            console.log('3. Open "LakehouseDataPopulation" notebook');
            console.log('4. Set MCPDemoLakehouse as default lakehouse');
            console.log('5. Click "Run all" to execute all cells');
        }
    }
}

// Run the function
runNotebookInFabric().catch(console.error);
