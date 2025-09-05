#!/usr/bin/env node

/**
 * Simple Getting Started Example - Microsoft Fabric MCP Server
 * 
 * This script demonstrates the basic workflow for:
 * 1. Connecting to the MCP server
 * 2. Listing available tools
 * 3. Working with a Fabric workspace
 * 4. Creating a simple lakehouse
 * 
 * Prerequisites:
 * - Azure CLI authenticated (az login)
 * - Microsoft Fabric workspace access
 * - Node.js environment
 */

const { spawn } = require('child_process');

// Configuration - Update with your workspace ID
const config = {
    workspaceId: process.env.FABRIC_DEFAULT_WORKSPACE_ID || "YOUR_WORKSPACE_ID_HERE", // Replace with your actual workspace ID
    lakehouseName: "MCPDemoLakehouse",
    description: "Demo lakehouse created via MCP automation"
};

// Path to the Fabric MCP Server - update this to your installation location
const MCP_SERVER_PATH = process.env.FABRIC_MCP_SERVER_PATH || 'C:\\Repos\\Fabric-Analytics-MCP';

let mcpProcess;

function cleanup() {
    if (mcpProcess) {
        console.log('🧹 Cleaning up...');
        mcpProcess.kill();
    }
}

process.on('exit', cleanup);
process.on('SIGINT', cleanup);
process.on('SIGTERM', cleanup);

async function sendMCPRequest(method, params = {}) {
    return new Promise((resolve, reject) => {
        const request = {
            jsonrpc: "2.0",
            id: Date.now(),
            method,
            params
        };

        let response = '';
        let resolved = false;

        const timeout = setTimeout(() => {
            if (!resolved) {
                resolved = true;
                reject(new Error('Request timeout'));
            }
        }, 15000);

        const dataHandler = (data) => {
            if (resolved) return;
            
            response += data.toString();
            const lines = response.split('\n');
            
            for (const line of lines) {
                if (line.trim()) {
                    try {
                        const parsed = JSON.parse(line);
                        if (parsed.id === request.id) {
                            resolved = true;
                            clearTimeout(timeout);
                            resolve(parsed);
                            return;
                        }
                    } catch (e) {
                        // Continue parsing other lines
                    }
                }
            }
        };

        mcpProcess.stdout.on('data', dataHandler);
        mcpProcess.stdin.write(JSON.stringify(request) + '\n');
    });
}

async function gettingStartedDemo() {
    console.log('🚀 Microsoft Fabric MCP Server - Getting Started Demo');
    console.log('='.repeat(60));
    
    if (config.workspaceId === "YOUR_WORKSPACE_ID_HERE") {
        console.log('❌ Please update config.workspaceId with your actual workspace ID');
        console.log('💡 You can find your workspace ID in the Fabric portal URL');
        return;
    }
    
    // Start MCP server
    console.log('🔧 Starting MCP server...');
    mcpProcess = spawn('node', ['build/index.js'], {
        stdio: ['pipe', 'pipe', 'pipe'],
        env: {
            ...process.env,
            FABRIC_AUTH_METHOD: 'azure_cli',
            FABRIC_DEFAULT_WORKSPACE_ID: config.workspaceId
        },
        cwd: MCP_SERVER_PATH
    });

    // Wait for server to start
    await new Promise(resolve => setTimeout(resolve, 3000));

    try {
        console.log('\n📋 Step 1: Check available tools...');
        const toolsResponse = await sendMCPRequest('tools/list');
        
        if (toolsResponse.result && toolsResponse.result.tools) {
            const tools = toolsResponse.result.tools;
            console.log(`✅ Found ${tools.length} MCP tools available`);
            
            // Show key tool categories
            const categories = {
                'Workspace': tools.filter(t => t.name.includes('workspace')),
                'Lakehouse': tools.filter(t => t.name.includes('lakehouse')),
                'Notebook': tools.filter(t => t.name.includes('notebook')),
                'Spark': tools.filter(t => t.name.includes('spark')),
            };
            
            Object.entries(categories).forEach(([category, categoryTools]) => {
                console.log(`   📁 ${category}: ${categoryTools.length} tools`);
            });
        }
        
        console.log('\n🏢 Step 2: Check workspace access...');
        const workspaceResponse = await sendMCPRequest('tools/call', {
            name: 'fabric_find_workspace',
            arguments: {
                searchName: config.workspaceId.substring(0, 8) // Search by first part of ID
            }
        });
        
        if (workspaceResponse.result) {
            console.log('✅ Workspace access confirmed');
            console.log(`   Workspace: ${config.workspaceId}`);
        }
        
        console.log('\n📦 Step 3: List existing items...');
        const itemsResponse = await sendMCPRequest('tools/call', {
            name: 'list-fabric-items',
            arguments: {
                workspaceId: config.workspaceId,
                itemType: 'Lakehouse'
            }
        });
        
        if (itemsResponse.result && itemsResponse.result.content) {
            console.log('✅ Current lakehouses:');
            console.log(itemsResponse.result.content[0].text);
        }
        
        console.log('\n🏗️ Step 4: Create demo lakehouse...');
        const createResponse = await sendMCPRequest('tools/call', {
            name: 'create-fabric-item',
            arguments: {
                workspaceId: config.workspaceId,
                itemType: 'Lakehouse',
                displayName: config.lakehouseName,
                description: config.description
            }
        });
        
        if (createResponse.result && createResponse.result.content) {
            console.log('✅ Lakehouse creation result:');
            console.log(createResponse.result.content[0].text);
        }
        
        console.log('\n🎉 Demo completed successfully!');
        console.log('\n💡 Next steps:');
        console.log('   1. Check your Fabric workspace in the portal');
        console.log('   2. Explore the examples/data-population/ folder');
        console.log('   3. Try the troubleshooting examples if needed');
        console.log(`   4. Portal URL: https://app.fabric.microsoft.com/groups/${config.workspaceId}`);
        
    } catch (error) {
        console.error('❌ Error during demo:', error.message);
        
        console.log('\n🔧 Troubleshooting tips:');
        console.log('   1. Ensure Azure CLI is authenticated: az login');
        console.log('   2. Check workspace ID is correct');
        console.log('   3. Verify you have access to the workspace');
        console.log('   4. Run examples/troubleshooting/ scripts for diagnosis');
    } finally {
        cleanup();
    }
}

// Run the demo
if (require.main === module) {
    gettingStartedDemo().catch(console.error);
}
