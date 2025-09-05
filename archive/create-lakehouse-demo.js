#!/usr/bin/env node

/**
 * Demo script to create a lakehouse with sample data using the MCP server
 */

import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

class MCPClient {
    constructor() {
        this.mcpProcess = null;
        this.requestId = 1;
    }

    async start() {
        return new Promise((resolve, reject) => {
            // Start the MCP server
            this.mcpProcess = spawn('node', [join(__dirname, 'build', 'index.js')], {
                stdio: ['pipe', 'pipe', 'pipe'],
                env: {
                    ...process.env,
                    FABRIC_AUTH_METHOD: 'azure_cli',
                    FABRIC_DEFAULT_WORKSPACE_ID: 'bcb44215-0e69-46d3-aac9-fb92fadcd982'
                }
            });

            this.mcpProcess.stdout.on('data', (data) => {
                const output = data.toString();
                console.log('MCP Server:', output.trim());
                if (output.includes('Microsoft Fabric Analytics MCP Server running')) {
                    resolve();
                }
            });

            this.mcpProcess.stderr.on('data', (data) => {
                console.error('MCP Error:', data.toString());
            });

            this.mcpProcess.on('error', reject);
        });
    }

    async sendRequest(method, params = {}) {
        const request = {
            jsonrpc: "2.0",
            id: this.requestId++,
            method: method,
            params: params
        };

        return new Promise((resolve, reject) => {
            let responseBuffer = '';
            
            const onData = (data) => {
                responseBuffer += data.toString();
                
                // Try to parse complete JSON responses
                const lines = responseBuffer.split('\n');
                for (let i = 0; i < lines.length - 1; i++) {
                    const line = lines[i].trim();
                    if (line) {
                        try {
                            const response = JSON.parse(line);
                            if (response.id === request.id) {
                                this.mcpProcess.stdout.off('data', onData);
                                resolve(response);
                                return;
                            }
                        } catch (e) {
                            // Not a complete JSON yet, continue
                        }
                    }
                }
                
                // Keep the last incomplete line
                responseBuffer = lines[lines.length - 1];
            };

            this.mcpProcess.stdout.on('data', onData);
            
            // Send the request
            this.mcpProcess.stdin.write(JSON.stringify(request) + '\n');
            
            setTimeout(() => {
                this.mcpProcess.stdout.off('data', onData);
                reject(new Error('Request timeout'));
            }, 30000);
        });
    }

    async stop() {
        if (this.mcpProcess) {
            this.mcpProcess.kill();
        }
    }
}

async function createLakehouseWithSampleData() {
    const client = new MCPClient();
    
    try {
        console.log('🚀 Starting MCP Server...');
        await client.start();
        
        console.log('📊 Creating lakehouse...');
        
        // Create a lakehouse
        const createLakehouseResponse = await client.sendRequest('tools/call', {
            name: 'fabric_create_item',
            arguments: {
                workspaceId: 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
                displayName: 'SampleDataLakehouse',
                type: 'Lakehouse',
                description: 'A sample lakehouse with demo data for testing Microsoft Fabric Analytics MCP'
            }
        });
        
        console.log('✅ Lakehouse created:', createLakehouseResponse);
        
        if (createLakehouseResponse.result) {
            const lakehouseId = createLakehouseResponse.result.content[0].text.match(/"id":\s*"([^"]+)"/)?.[1];
            
            if (lakehouseId) {
                console.log(`📦 Lakehouse ID: ${lakehouseId}`);
                
                // Create a notebook to generate sample data
                console.log('📓 Creating sample data notebook...');
                const createNotebookResponse = await client.sendRequest('tools/call', {
                    name: 'fabric_create_notebook',
                    arguments: {
                        workspaceId: 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
                        displayName: 'SampleDataGenerator',
                        description: 'Notebook to generate sample data for the lakehouse'
                    }
                });
                
                console.log('✅ Sample data notebook created:', createNotebookResponse);
                
                // List workspace items to verify creation
                console.log('📋 Listing workspace items...');
                const listItemsResponse = await client.sendRequest('tools/call', {
                    name: 'fabric_list_workspace_items',
                    arguments: {
                        workspaceId: 'bcb44215-0e69-46d3-aac9-fb92fadcd982'
                    }
                });
                
                console.log('✅ Workspace items:', listItemsResponse);
            }
        }
        
    } catch (error) {
        console.error('❌ Error:', error);
    } finally {
        await client.stop();
    }
}

// Run the demo
createLakehouseWithSampleData();
