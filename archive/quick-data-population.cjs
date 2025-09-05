const { spawn } = require('child_process');

const config = {
    workspaceId: "bcb44215-0e69-46d3-aac9-fb92fadcd982",
    lakehouseId: "5e6b33fe-1f33-419a-a954-bce697ccfe61"
};

async function getAzureToken() {
    const { execSync } = require('child_process');
    try {
        const result = execSync('az account get-access-token --resource "https://analysis.windows.net/powerbi/api"', { encoding: 'utf8' });
        const tokenData = JSON.parse(result);
        return tokenData.accessToken;
    } catch (error) {
        console.error('Failed to get Azure token:', error.message);
        return null;
    }
}

async function sendMCPRequest(tool, params, timeout = 120000) {
    return new Promise((resolve, reject) => {
        const timeoutHandle = setTimeout(() => {
            reject(new Error(`Request timeout for ${tool}`));
        }, timeout);

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

        console.log(`📤 ${tool}...`);

        let responseReceived = false;

        const dataHandler = (data) => {
            if (responseReceived) return;
            
            const output = data.toString();
            
            try {
                const lines = output.split('\n');
                for (const line of lines) {
                    if (line.trim()) {
                        const parsed = JSON.parse(line);
                        if (parsed.id === requestId) {
                            responseReceived = true;
                            clearTimeout(timeoutHandle);
                            
                            if (parsed.error) {
                                reject(new Error(`MCP error: ${parsed.error.message}`));
                            } else {
                                resolve(parsed.result);
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

        mcpProcess.stdin.write(JSON.stringify(request) + '\n');
    });
}

let mcpProcess;

async function quickDataPopulation() {
    console.log('🚀 Quick Lakehouse Data Population');
    console.log('='.repeat(40));
    
    const token = await getAzureToken();
    if (!token) {
        console.error('❌ Failed to get Azure token');
        return;
    }
    
    console.log(`✅ Got Azure token`);
    
    mcpProcess = spawn('node', ['C:\\Repos\\Fabric-Analytics-MCP\\build\\index.js'], {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: 'C:\\Repos\\Fabric-Analytics-MCP'
    });

    await new Promise(resolve => setTimeout(resolve, 3000));

    try {
        console.log('\n🚀 Creating Livy session...');
        const sessionResult = await sendMCPRequest('create-livy-session', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionConfig: {
                kind: 'pyspark'
            }
        });
        
        const sessionData = JSON.parse(sessionResult.content[0].text);
        const sessionId = sessionData.id;
        console.log(`✅ Created session: ${sessionId}`);
        
        // Wait a bit and then try to execute
        console.log('⏳ Waiting 60 seconds for session to initialize...');
        await new Promise(resolve => setTimeout(resolve, 60000));
        
        console.log('\n🚀 Executing data creation code...');
        const dataCode = `
# Quick data population for Fabric lakehouse
print("🔄 Starting data population...")

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import random

# Get active Spark session
spark = SparkSession.getActiveSession()
print(f"✅ Using Spark {spark.version}")

# Create simple demo data
print("📊 Creating demo sales data...")

# Generate 100 sales records
sales_data = []
for i in range(100):
    sales_data.append((
        i + 1,                           # transaction_id
        f"CUST_{random.randint(1,50)}",  # customer_id
        f"PROD_{random.randint(1,20)}",  # product_id
        random.randint(1, 5),            # quantity
        round(random.uniform(10, 500), 2) # amount
    ))

# Create DataFrame
sales_schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("amount", DoubleType(), False)
])

sales_df = spark.createDataFrame(sales_data, sales_schema)
print(f"✅ Created {sales_df.count()} sales records")

# Save to Delta table
print("💾 Saving to Delta table 'demo_sales'...")
sales_df.write.format("delta").mode("overwrite").saveAsTable("demo_sales")

print("✅ SUCCESS! Data saved to lakehouse")
print("📋 Verifying data...")

# Verify
count = spark.sql("SELECT COUNT(*) as total FROM demo_sales").collect()[0]['total']
print(f"📊 Verified: {count} records in demo_sales table")

# Show sample
print("📄 Sample data:")
spark.sql("SELECT * FROM demo_sales LIMIT 5").show()

print("🎉 Data population completed successfully!")
`;
        
        const execResult = await sendMCPRequest('execute-livy-statement', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionId: sessionId,
            code: dataCode,
            kind: 'pyspark'
        });
        
        const resultText = execResult.content[0].text;
        console.log('\n📋 Execution Result:');
        console.log(resultText);
        
        if (resultText.includes('Data population completed successfully')) {
            console.log('\n🎉 SUCCESS! Your Fabric lakehouse has been populated with demo data!');
            console.log('📊 Table created: demo_sales (100 records)');
            console.log('🔍 You can now query this data in Fabric or connect to it from other tools');
        } else {
            console.log('\n⚠️ Execution completed but check the output above for any issues');
        }
        
    } catch (error) {
        console.error('❌ Error:', error.message);
    }

    console.log('\n🛑 Cleaning up...');
    mcpProcess.kill();
}

quickDataPopulation().catch(console.error);
