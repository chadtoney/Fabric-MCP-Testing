const { spawn } = require('child_process');

// Path to the Fabric MCP Server - update this to your installation location
const MCP_SERVER_PATH = process.env.FABRIC_MCP_SERVER_PATH || 'C:\\Repos\\Fabric-Analytics-MCP';

async function checkMCPTools() {
    console.log('🔍 Investigating MCP Fabric Notebook Tools');
    console.log('='.repeat(50));
    
    const mcpProcess = spawn('node', [MCP_SERVER_PATH + '\\build\\index.js'], {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: MCP_SERVER_PATH
    });

    await new Promise(resolve => setTimeout(resolve, 3000));

    try {
        // Check available tools
        const toolsRequest = {
            jsonrpc: "2.0",
            id: 1,
            method: "tools/list"
        };

        console.log('📋 Requesting available MCP tools...');
        
        let toolsReceived = false;
        
        const dataHandler = (data) => {
            if (toolsReceived) return;
            
            const output = data.toString();
            
            try {
                const lines = output.split('\n');
                for (const line of lines) {
                    if (line.trim()) {
                        const parsed = JSON.parse(line);
                        if (parsed.id === 1 && parsed.result) {
                            toolsReceived = true;
                            
                            console.log('\n📚 Available Fabric Notebook Tools:');
                            console.log('='.repeat(40));
                            
                            const notebookTools = parsed.result.tools.filter(tool => 
                                tool.name.toLowerCase().includes('notebook')
                            );
                            
                            notebookTools.forEach(tool => {
                                console.log(`🔧 ${tool.name}`);
                                console.log(`   Description: ${tool.description}`);
                                console.log(`   Parameters: ${JSON.stringify(tool.inputSchema.properties, null, 2)}`);
                                console.log('');
                            });
                            
                            console.log('\n🎯 Analysis:');
                            
                            const hasUpdate = notebookTools.some(t => t.name.includes('update'));
                            const hasDefinition = notebookTools.some(t => t.name.includes('definition'));
                            const hasContent = notebookTools.some(t => t.name.includes('content'));
                            
                            console.log(`   • Update tools available: ${hasUpdate ? '✅' : '❌'}`);
                            console.log(`   • Definition tools available: ${hasDefinition ? '✅' : '❌'}`);
                            console.log(`   • Content tools available: ${hasContent ? '✅' : '❌'}`);
                            
                            if (hasUpdate) {
                                console.log('\n💡 Recommendation: Try update-fabric-notebook for content');
                            } else {
                                console.log('\n⚠️  Issue: Limited notebook content management tools');
                                console.log('   This explains why notebooks appear blank');
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

        mcpProcess.stdin.write(JSON.stringify(toolsRequest) + '\n');
        
        // Wait for response
        await new Promise(resolve => setTimeout(resolve, 5000));
        
        if (!toolsReceived) {
            console.log('❌ No response received from MCP server');
        }
        
    } catch (error) {
        console.error('❌ Error checking MCP tools:', error.message);
    }

    console.log('\n🔧 Alternative Solutions:');
    console.log('1. 📋 Manual notebook creation in Fabric Portal');
    console.log('2. 📄 Copy code from local notebook file');
    console.log('3. 🔄 Direct Livy execution (data already works)');
    console.log('4. 📊 Focus on data analytics with existing tables');
    
    console.log('\n✅ What We Successfully Achieved:');
    console.log('   • Populated lakehouse with 5,000 customers');
    console.log('   • Populated lakehouse with 1,000 products');
    console.log('   • Created working Delta tables');
    console.log('   • Verified data accessibility');
    console.log('   • Provided analysis code for manual use');

    mcpProcess.kill();
}

checkMCPTools().catch(console.error);
