const { spawn } = require('child_process');

// Configuration
const config = {
  workspaceId: process.env.FABRIC_DEFAULT_WORKSPACE_ID || 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
  bearerToken: 'azure_cli',
  lakehouseName: 'MCPDemoLakehouse', // Simplified name without spaces
  lakehouseDescription: 'Demo lakehouse created via MCP server'
};

console.log('🚀 Starting Microsoft Fabric MCP Lakehouse Demo (Fixed)');
console.log(`Workspace: ${config.workspaceId}`);
console.log(`Lakehouse Name: ${config.lakehouseName}`);

// Start the MCP server process
const mcpServer = spawn('node', ['build/index.js'], {
  stdio: ['pipe', 'pipe', 'pipe'],
  env: {
    ...process.env,
    FABRIC_AUTH_METHOD: 'azure_cli',
    FABRIC_DEFAULT_WORKSPACE_ID: config.workspaceId
  }
});

let requestId = 1;
let responseBuffer = '';

function sendMcpRequest(method, params = {}) {
  const request = {
    jsonrpc: '2.0',
    id: requestId++,
    method: method,
    params: params
  };
  
  console.log(`📤 Sending MCP request: ${method}`);
  mcpServer.stdin.write(JSON.stringify(request) + '\n');
  
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`Request ${method} timed out after 30 seconds`));
    }, 30000);
    
    function processResponse(data) {
      responseBuffer += data.toString();
      const lines = responseBuffer.split('\n');
      
      for (let i = 0; i < lines.length - 1; i++) {
        const line = lines[i].trim();
        if (line) {
          try {
            const response = JSON.parse(line);
            if (response.id === request.id) {
              clearTimeout(timeout);
              mcpServer.stdout.removeListener('data', processResponse);
              responseBuffer = lines[lines.length - 1];
              resolve(response);
              return;
            }
          } catch (error) {
            // Ignore JSON parse errors for incomplete lines
          }
        }
      }
      responseBuffer = lines[lines.length - 1];
    }
    
    mcpServer.stdout.on('data', processResponse);
  });
}

async function createLakehouseDemo() {
  try {
    console.log('\n🏗️ Creating lakehouse with simplified name...');
    const createResponse = await sendMcpRequest('tools/call', {
      name: 'create-fabric-item',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        itemType: 'Lakehouse',
        displayName: config.lakehouseName,
        description: config.lakehouseDescription
      }
    });
    
    console.log('✅ Create response:');
    console.log(JSON.stringify(createResponse, null, 2));
    
    // Extract lakehouse ID if successful
    if (createResponse.result && createResponse.result.content) {
      const content = createResponse.result.content[0].text;
      
      if (content.includes('Successfully created')) {
        const idMatch = content.match(/ID: ([a-f0-9-]+)/);
        if (idMatch) {
          const lakehouseId = idMatch[1];
          console.log(`🎯 Created lakehouse with ID: ${lakehouseId}`);
          
          console.log('\n📋 Listing items to confirm creation...');
          const listResponse = await sendMcpRequest('tools/call', {
            name: 'list-fabric-items',
            arguments: {
              bearerToken: config.bearerToken,
              workspaceId: config.workspaceId,
              itemType: 'Lakehouse'
            }
          });
          
          console.log('✅ Updated lakehouse list:');
          console.log(JSON.stringify(listResponse, null, 2));
          
          console.log('\n📊 Creating sample notebook...');
          const notebookResponse = await sendMcpRequest('tools/call', {
            name: 'create-fabric-notebook',
            arguments: {
              bearerToken: config.bearerToken,
              workspaceId: config.workspaceId,
              displayName: 'SampleDataGenerator',
              description: 'Notebook to generate sample data for the lakehouse'
            }
          });
          
          console.log('✅ Notebook creation response:');
          console.log(JSON.stringify(notebookResponse, null, 2));
          
          console.log('\n🎉 Demo completed successfully!');
          console.log(`✅ Created lakehouse: ${config.lakehouseName} (ID: ${lakehouseId})`);
          console.log(`🌐 Workspace URL: https://msit.powerbi.com/groups/${config.workspaceId}`);
          console.log('\n📍 Next steps:');
          console.log('1. Open Microsoft Fabric portal');
          console.log('2. Navigate to your workspace');
          console.log('3. Find the new lakehouse and notebook');
          console.log('4. Use the notebook to create sample data tables');
        }
      } else {
        console.log('❌ Creation failed. Response content:', content);
      }
    }
    
  } catch (error) {
    console.error('❌ Error in demo:', error.message);
  } finally {
    console.log('\n🛑 Stopping MCP server...');
    mcpServer.kill();
    setTimeout(() => process.exit(0), 1000);
  }
}

// Handle MCP server startup
mcpServer.stderr.on('data', (data) => {
  const message = data.toString();
  console.log('🔧 MCP Server:', message.trim());
  
  if (message.includes('Microsoft Fabric Analytics MCP Server running')) {
    console.log('✅ MCP Server started successfully');
    setTimeout(createLakehouseDemo, 2000);
  }
});

mcpServer.stdout.on('data', (data) => {
  const message = data.toString();
  if (!message.trim().startsWith('{')) {
    console.log('📤 MCP Output:', message.trim());
  }
});

mcpServer.on('error', (error) => {
  console.error('❌ MCP Server error:', error);
  process.exit(1);
});

mcpServer.on('exit', (code) => {
  console.log(`MCP Server exited with code ${code}`);
});
