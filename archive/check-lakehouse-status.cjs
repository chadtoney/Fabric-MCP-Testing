const { spawn } = require('child_process');

// Configuration
const config = {
  workspaceId: process.env.FABRIC_DEFAULT_WORKSPACE_ID || 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
  bearerToken: 'azure_cli',
  lakehouseId: '5e6b33fe-1f33-419a-a954-bce697ccfe61' // ID from the created lakehouse
};

console.log('🔍 Checking Microsoft Fabric Lakehouse Status via MCP');
console.log(`Workspace: ${config.workspaceId}`);
console.log(`Lakehouse: ${config.lakehouseId}`);

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

async function checkLakehouseStatus() {
  try {
    console.log('\n🔄 Step 1: Listing workspace items...');
    const itemsResponse = await sendMcpRequest('tools/call', {
      name: 'list-fabric-items',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId
      }
    });
    
    console.log('✅ Workspace items response:');
    const itemsContent = itemsResponse.result.content[0].text;
    console.log(itemsContent);
    
    console.log('\n🔄 Step 2: Trying to get lakehouse details...');
    const lakehouseResponse = await sendMcpRequest('tools/call', {
      name: 'get-fabric-item',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        itemId: config.lakehouseId
      }
    });
    
    console.log('✅ Lakehouse details response:');
    const lakehouseContent = lakehouseResponse.result.content[0].text;
    console.log(lakehouseContent);
    
    console.log('\n🔄 Step 3: Checking active Livy sessions...');
    const sessionsResponse = await sendMcpRequest('tools/call', {
      name: 'list-livy-sessions',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId
      }
    });
    
    console.log('✅ Active sessions response:');
    const sessionsContent = sessionsResponse.result.content[0].text;
    console.log(sessionsContent);
    
    console.log('\n📊 Summary:');
    console.log(`✅ Lakehouse: MCPDemoLakehouse (${config.lakehouseId})`);
    console.log(`🌐 Portal URL: https://msit.powerbi.com/groups/${config.workspaceId}`);
    console.log('ℹ️ Check the portal for any existing tables or data');
    console.log('💡 The lakehouse exists and is accessible via the MCP server');
    
  } catch (error) {
    console.error('❌ Error checking lakehouse status:', error.message);
  } finally {
    console.log('\n🛑 Stopping MCP server...');
    mcpServer.kill();
    setTimeout(() => process.exit(0), 2000);
  }
}

// Handle MCP server startup
mcpServer.stderr.on('data', (data) => {
  const message = data.toString();
  console.log('🔧 MCP Server:', message.trim());
  
  if (message.includes('Microsoft Fabric Analytics MCP Server running')) {
    console.log('✅ MCP Server started successfully');
    setTimeout(checkLakehouseStatus, 2000);
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
