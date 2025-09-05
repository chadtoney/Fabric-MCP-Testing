#!/usr/bin/env node

/**
 * Test script to replicate Issue #12 - Capacity management tools not recognized
 * This simulates what gbrueckl experienced when asking VS Code agent for Fabric capacities
 */

const { spawn } = require('child_process');

// Configuration - Update with your workspace ID
const config = {
    // Get your workspace ID from Fabric portal URL: https://app.fabric.microsoft.com/groups/YOUR-WORKSPACE-ID
    workspaceId: process.env.FABRIC_DEFAULT_WORKSPACE_ID || "YOUR_WORKSPACE_ID_HERE"
};

let mcpProcess;

function cleanup() {
    if (mcpProcess) {
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
        }, 10000);

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

// Path to the Fabric MCP Server - update this to your installation location
const MCP_SERVER_PATH = process.env.FABRIC_MCP_SERVER_PATH || 'C:\\Repos\\Fabric-Analytics-MCP';

async function testCapacityIssue() {
    console.log('🔍 Testing Issue #12 - Capacity Management Tools Not Recognized');
    console.log('='.repeat(70));
    
    if (config.workspaceId === "YOUR_WORKSPACE_ID_HERE") {
        console.log('❌ Please update config.workspaceId with your actual workspace ID');
        console.log('💡 Find it in your Fabric portal URL: https://app.fabric.microsoft.com/groups/YOUR-WORKSPACE-ID');
        return;
    }
    
    // Start MCP server
    console.log('🚀 Starting MCP server...');
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
        console.log('\n📋 Step 1: Get available tools...');
        const toolsResponse = await sendMCPRequest('tools/list');
        
        if (toolsResponse.result && toolsResponse.result.tools) {
            const allTools = toolsResponse.result.tools;
            console.log(`✅ Found ${allTools.length} total tools`);
            
            // Search for capacity-related tools
            const capacityTools = allTools.filter(tool => 
                tool.name.toLowerCase().includes('capacity') ||
                tool.description.toLowerCase().includes('capacity')
            );
            
            console.log(`\n🔍 Capacity-related tools found: ${capacityTools.length}`);
            
            if (capacityTools.length === 0) {
                console.log('❌ NO CAPACITY MANAGEMENT TOOLS FOUND!');
                console.log('   This confirms Issue #12 - capacity tools are missing');
                
                // Show workspace tools instead (what it falls back to)
                const workspaceTools = allTools.filter(tool => 
                    tool.name.toLowerCase().includes('workspace')
                );
                
                console.log(`\n📋 Workspace tools available (${workspaceTools.length}):`);
                workspaceTools.forEach(tool => {
                    console.log(`   - ${tool.name}: ${tool.description}`);
                });
                
            } else {
                console.log('\n📋 Capacity tools found:');
                capacityTools.forEach(tool => {
                    console.log(`   - ${tool.name}: ${tool.description}`);
                });
            }
            
            console.log('\n🧪 Step 2: Test asking for capacities...');
            
            // Try to simulate what a user would ask
            console.log('❓ User request: "Give me a list of Fabric capacities"');
            
            // Since no capacity tools exist, let's see what tools ARE available for listing things
            const listTools = allTools.filter(tool => 
                tool.name.toLowerCase().includes('list') ||
                tool.name.toLowerCase().includes('get')
            );
            
            console.log(`\n📝 Available "list" tools (${listTools.length}):`);
            listTools.slice(0, 10).forEach(tool => {
                console.log(`   - ${tool.name}: ${tool.description}`);
            });
            
            // Show what it would fall back to
            const workspaceListTool = allTools.find(tool => 
                tool.name === 'fabric_list_workspaces' || 
                tool.name === 'list-fabric-workspaces'
            );
            
            if (workspaceListTool) {
                console.log(`\n⚠️  ISSUE CONFIRMED: Would fall back to "${workspaceListTool.name}"`);
                console.log(`   Description: ${workspaceListTool.description}`);
                console.log('   This matches the behavior described in Issue #12!');
            }
            
        } else {
            console.log('❌ Failed to get tools list');
        }
        
    } catch (error) {
        console.error('❌ Error during test:', error.message);
    } finally {
        console.log('\n🏁 Test completed');
        cleanup();
    }
}

// Run the test
testCapacityIssue().catch(console.error);
