const { spawn } = require('child_process');

// Configuration
const config = {
  workspaceId: 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
  notebookId: '1df1becd-d296-4212-9e20-dceb390d3994',
  bearerToken: 'azure_cli' // This will trigger Azure CLI authentication
};

console.log('🔍 Checking available MCP tools for notebook execution...');
console.log(`Workspace: ${config.workspaceId}`);
console.log(`Notebook: ${config.notebookId}`);

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
  
  console.log(`📤 Sending: ${JSON.stringify(request)}`);
  mcpServer.stdin.write(JSON.stringify(request) + '\n');
  
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error('Request timeout'));
    }, 30000);
    
    function handleResponse(data) {
      responseBuffer += data;
      
      const lines = responseBuffer.split('\n');
      responseBuffer = lines.pop() || '';
      
      for (const line of lines) {
        if (!line.trim()) continue;
        
        try {
          const response = JSON.parse(line);
          if (response.id === request.id) {
            clearTimeout(timeout);
            mcpServer.stdout.removeListener('data', handleResponse);
            console.log(`📥 Received: ${JSON.stringify(response, null, 2)}`);
            resolve(response);
            return;
          }
        } catch (err) {
          // Ignore parsing errors for intermediate messages
        }
      }
    }
    
    mcpServer.stdout.on('data', handleResponse);
  });
}

async function checkToolsAndRunNotebook() {
  try {
    // Wait for server to start
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    console.log('\n🔍 Step 1: List available tools...');
    const toolsResponse = await sendMcpRequest('tools/list');
    
    if (toolsResponse.result && toolsResponse.result.tools) {
      const notebookTools = toolsResponse.result.tools.filter(tool => 
        tool.name.includes('notebook') || tool.name.includes('execute') || tool.name.includes('run')
      );
      
      console.log('\n📋 Available notebook-related tools:');
      notebookTools.forEach(tool => {
        console.log(`   - ${tool.name}: ${tool.description}`);
      });
      
      // Try to run the notebook with the most appropriate tool
      if (notebookTools.some(t => t.name === 'run-fabric-notebook')) {
        console.log('\n🚀 Step 2: Running notebook using run-fabric-notebook...');
        
        const runResponse = await sendMcpRequest('tools/call', {
          name: 'run-fabric-notebook',
          arguments: {
            bearerToken: config.bearerToken,
            workspaceId: config.workspaceId,
            notebookId: config.notebookId,
            parameters: {}
          }
        });
        
        console.log('\n✅ Notebook execution response:');
        console.log(JSON.stringify(runResponse, null, 2));
        
      } else if (notebookTools.some(t => t.name === 'execute-fabric-notebook')) {
        console.log('\n🚀 Step 2: Running notebook using execute-fabric-notebook...');
        
        const executeResponse = await sendMcpRequest('tools/call', {
          name: 'execute-fabric-notebook',
          arguments: {
            bearerToken: config.bearerToken,
            workspaceId: config.workspaceId,
            notebookId: config.notebookId,
            parameters: {}
          }
        });
        
        console.log('\n✅ Notebook execution response:');
        console.log(JSON.stringify(executeResponse, null, 2));
        
      } else {
        console.log('\n❌ No suitable notebook execution tools found');
        console.log('Available tools:', notebookTools.map(t => t.name));
      }
      
    } else {
      console.log('❌ Failed to get tools list');
      console.log('Response:', toolsResponse);
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  } finally {
    mcpServer.kill();
    console.log('\n🏁 Process completed');
  }
}

// Handle server errors
mcpServer.stderr.on('data', (data) => {
  const output = data.toString();
  if (output.includes('Microsoft Fabric Analytics MCP Server running')) {
    console.log('✅ MCP Server is ready');
  } else if (output.includes('error') || output.includes('Error')) {
    console.error('Server error:', output);
  }
});

mcpServer.on('close', (code) => {
  console.log(`Server process exited with code ${code}`);
});

// Start the process
checkToolsAndRunNotebook();
