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

async function waitForSessionReady(token, sessionId, maxWaitSeconds = 120) {
    console.log(`⏳ Waiting for session ${sessionId} to be ready...`);
    
    for (let i = 0; i < maxWaitSeconds; i += 10) {
        try {
            const sessionStatus = await sendMCPRequest('get-livy-session', {
                bearerToken: token,
                workspaceId: config.workspaceId,
                lakehouseId: config.lakehouseId,
                sessionId: sessionId
            });
            
            const statusText = sessionStatus.content[0].text;
            console.log(`📊 Session status: ${statusText.substring(0, 100)}...`);
            
            if (statusText.includes('"state":"idle"') || statusText.includes('"state":"Idle"')) {
                console.log('✅ Session is ready!');
                return true;
            }
            
            console.log(`⏳ Still waiting... (${i + 10}s elapsed)`);
            await new Promise(resolve => setTimeout(resolve, 10000)); // Wait 10 seconds
            
        } catch (error) {
            console.log(`⚠️ Error checking session status: ${error.message}`);
            await new Promise(resolve => setTimeout(resolve, 10000));
        }
    }
    
    console.log('❌ Session did not become ready in time');
    return false;
}

let mcpProcess;

async function runDataPopulation() {
    console.log('🚀 Fabric Lakehouse Data Population via MCP');
    console.log('='.repeat(50));
    
    // Get real Azure token
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

    // Wait for startup
    console.log('⏳ Starting MCP server...');
    await new Promise(resolve => setTimeout(resolve, 3000));

    try {
        console.log('\n🚀 Step 1: Creating Livy session...');
        const sessionResult = await sendMCPRequest('create-livy-session', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionConfig: {
                kind: 'pyspark',
                name: 'MCPDataPopulation'
            }
        });
        
        const sessionResponseText = sessionResult.content[0].text;
        const sessionData = JSON.parse(sessionResponseText);
        const sessionId = sessionData.id;
        
        console.log(`✅ Created session: ${sessionId}`);
        
        // Wait for session to be ready
        const isReady = await waitForSessionReady(token, sessionId);
        if (!isReady) {
            throw new Error('Session did not become ready');
        }
        
        console.log('\n🚀 Step 2: Setting up Spark environment...');
        const setupCode = `
# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

# Get Spark session
spark = SparkSession.getActiveSession()
print("✅ Spark session active")
print(f"Spark version: {spark.version}")

# Show available databases
print("Available databases:")
spark.sql("SHOW DATABASES").show()
`;
        
        const setupResult = await sendMCPRequest('execute-livy-statement', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionId: sessionId,
            code: setupCode,
            kind: 'pyspark'
        });
        
        console.log('✅ Setup completed');
        
        console.log('\n🚀 Step 3: Creating sample customer data...');
        const customerCode = `
# Generate customer data
import random
from datetime import datetime, timedelta

print("🔄 Generating customer data...")

# Create customer data
customers_data = []
first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Robert', 'Jessica', 'William', 'Ashley']
last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']

for i in range(1000):  # Creating 1000 customers
    customer = (
        i + 1,  # customer_id
        random.choice(first_names),  # first_name
        random.choice(last_names),   # last_name
        f"user{i+1}@example.com",    # email
        random.choice(cities),       # city
        datetime.now() - timedelta(days=random.randint(1, 365)),  # registration_date
        random.choice([True, False]), # is_active
        round(random.uniform(100, 10000), 2)  # customer_value
    )
    customers_data.append(customer)

# Create DataFrame
from pyspark.sql.types import *
customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("city", StringType(), False),
    StructField("registration_date", TimestampType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("customer_value", DoubleType(), False)
])

customers_df = spark.createDataFrame(customers_data, customer_schema)
print(f"✅ Created {customers_df.count()} customer records")
customers_df.show(5)
`;
        
        const customerResult = await sendMCPRequest('execute-livy-statement', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionId: sessionId,
            code: customerCode,
            kind: 'pyspark'
        });
        
        console.log('✅ Customer data created');
        
        console.log('\n🚀 Step 4: Saving data to lakehouse...');
        const saveCode = `
# Save customer data to Delta table
print("💾 Saving customers to Delta table...")

customers_df.write \\
    .format("delta") \\
    .mode("overwrite") \\
    .option("mergeSchema", "true") \\
    .saveAsTable("customers")

print("✅ Customer data saved to 'customers' table")

# Verify the data
customer_count = spark.sql("SELECT COUNT(*) as count FROM customers").collect()[0]['count']
print(f"📊 Verified: {customer_count} customers in table")

# Show sample data
print("📋 Sample customer data:")
spark.sql("SELECT * FROM customers LIMIT 5").show()
`;
        
        const saveResult = await sendMCPRequest('execute-livy-statement', {
            bearerToken: token,
            workspaceId: config.workspaceId,
            lakehouseId: config.lakehouseId,
            sessionId: sessionId,
            code: saveCode,
            kind: 'pyspark'
        });
        
        console.log('✅ Data saved to lakehouse!');
        
        console.log('\n🎉 SUCCESS! Lakehouse populated with sample data');
        console.log('📋 Summary:');
        console.log('- Created Livy session');
        console.log('- Generated 1000 customer records');
        console.log('- Saved data to Delta table "customers"');
        console.log('- Data is now available in your Fabric lakehouse');
        
    } catch (error) {
        console.error('❌ Error:', error.message);
    }

    console.log('\n🛑 Cleaning up...');
    mcpProcess.kill();
}

runDataPopulation().catch(console.error);
