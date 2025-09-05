const { spawn } = require('child_process');

// Configuration
const config = {
  workspaceId: process.env.FABRIC_DEFAULT_WORKSPACE_ID || 'bcb44215-0e69-46d3-aac9-fb92fadcd982',
  bearerToken: 'azure_cli',
  lakehouseId: '5e6b33fe-1f33-419a-a954-bce697ccfe61' // ID from the created lakehouse
};

console.log('📊 Debug: Adding Sample Data to Microsoft Fabric Lakehouse via MCP');
console.log(`Workspace: ${config.workspaceId}`);
console.log(`Lakehouse: ${config.lakehouseId}`);

// Simple data generation script
const simpleDataCode = `
print("🚀 Starting simple data generation...")

# Create a small dataset
from pyspark.sql import Row
sales_data = []
for i in range(10):
    sales_data.append(Row(id=i, product=f"Product_{i}", amount=100.0 + i))

df = spark.createDataFrame(sales_data)
print(f"Created DataFrame with {df.count()} rows")
df.show()

# Save to table
df.write.format("delta").mode('overwrite').saveAsTable('test_sales')
print("✅ Saved test_sales table")
print("🎉 Simple data generation completed!")
`;

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
      reject(new Error(`Request ${method} timed out after 45 seconds`));
    }, 45000);
    
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

async function debugSampleData() {
  try {
    console.log('\n🔄 Step 1: Creating basic Livy session...');
    const sessionResponse = await sendMcpRequest('tools/call', {
      name: 'create-livy-session',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId,
        sessionConfig: {
          kind: 'pyspark',
          name: 'debug-session'
        }
      }
    });
    
    console.log('✅ Raw session response:');
    console.log(JSON.stringify(sessionResponse, null, 2));
    
    const sessionContent = sessionResponse.result.content[0].text;
    const sessionData = JSON.parse(sessionContent);
    const sessionId = sessionData.id;
    console.log(`🎯 Session ID: ${sessionId}`);
    
    // Wait for session
    console.log('\n⏳ Waiting 15 seconds for session...');
    await new Promise(resolve => setTimeout(resolve, 15000));
    
    console.log('\n📊 Step 2: Executing simple code...');
    const dataResponse = await sendMcpRequest('tools/call', {
      name: 'execute-livy-statement',
      arguments: {
        bearerToken: config.bearerToken,
        workspaceId: config.workspaceId,
        lakehouseId: config.lakehouseId,
        sessionId: sessionId,
        code: simpleDataCode,
        kind: 'pyspark'
      }
    });
    
    console.log('✅ Raw execution response:');
    console.log(JSON.stringify(dataResponse, null, 2));
    
    // Extract statement ID regardless of response structure
    let statementId = null;
    if (dataResponse && dataResponse.result) {
      if (dataResponse.result.content && dataResponse.result.content[0]) {
        const execContent = dataResponse.result.content[0].text;
        console.log('Execution content:', execContent);
        try {
          const execData = JSON.parse(execContent);
          statementId = execData.id;
          console.log(`🎯 Statement ID: ${statementId}, Status: ${execData.state}`);
        } catch (e) {
          console.log('❌ Could not parse execution content as JSON');
        }
      } else {
        console.log('❌ No content array in result');
      }
    } else {
      console.log('❌ No result in response');
    }
    
    if (statementId) {
      // Wait and check results
      console.log('\n⏳ Waiting 20 seconds for execution...');
      await new Promise(resolve => setTimeout(resolve, 20000));
      
      console.log('\n🔍 Step 3: Checking execution status...');
      const resultResponse = await sendMcpRequest('tools/call', {
        name: 'get-livy-statement',
        arguments: {
          bearerToken: config.bearerToken,
          workspaceId: config.workspaceId,
          lakehouseId: config.lakehouseId,
          sessionId: sessionId,
          statementId: statementId
        }
      });
      
      console.log('✅ Raw result response:');
      console.log(JSON.stringify(resultResponse, null, 2));
      
      if (resultResponse && resultResponse.result && resultResponse.result.content && resultResponse.result.content[0]) {
        const resultContent = resultResponse.result.content[0].text;
        const resultData = JSON.parse(resultContent);
        console.log(`📊 Final Status: ${resultData.state}`);
        
        if (resultData.output && resultData.output.data) {
          console.log('📋 Output:');
          console.log(resultData.output.data['text/plain']);
        }
      }
    } else {
      console.log('❌ Could not extract statement ID, skipping result check');
    }
    
    console.log('\n🎉 Debug process completed!');
    console.log(`🌐 Check your lakehouse: https://msit.powerbi.com/groups/${config.workspaceId}`);
    
  } catch (error) {
    console.error('❌ Error in debug process:', error.message);
    console.error('Stack:', error.stack);
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
    setTimeout(debugSampleData, 2000);
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
