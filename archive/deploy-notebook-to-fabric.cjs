const fs = require('fs');
const { execSync } = require('child_process');

async function deployNotebookToFabric() {
    try {
        console.log('🚀 Deploying notebook to Fabric workspace...');
        
        const workspaceId = 'bcb44215-0e69-46d3-aac9-fb92fadcd982';
        const notebookId = '1df1becd-d296-4212-9e20-dceb390d3994';
        
        // Read the notebook content
        console.log('📖 Reading notebook content...');
        const notebookContent = fs.readFileSync('fabric-lakehouse-data-population.ipynb', 'utf8');
        
        // Convert to base64 for upload
        const base64Content = Buffer.from(notebookContent).toString('base64');
        
        // Create temporary file for the content
        const tempFile = 'temp-notebook-content.json';
        const uploadPayload = {
            displayName: "LakehouseDataPopulation",
            description: "Comprehensive data population notebook for MCPDemoLakehouse with analytics tables",
            definition: {
                format: "ipynb",
                parts: [
                    {
                        path: "notebook-content.ipynb",
                        payload: base64Content,
                        payloadType: "InlineBase64"
                    }
                ]
            }
        };
        
        fs.writeFileSync(tempFile, JSON.stringify(uploadPayload, null, 2));
        
        console.log('📤 Uploading notebook to Fabric...');
        
        // Update the notebook using Azure CLI
        const updateCommand = `az rest --method POST --url "https://api.fabric.microsoft.com/v1/workspaces/${workspaceId}/items/${notebookId}/updateDefinition" --resource "https://api.fabric.microsoft.com" --body @${tempFile}`;
        
        try {
            const result = execSync(updateCommand, { encoding: 'utf8' });
            console.log('✅ Notebook updated successfully!');
            console.log('Response:', result);
        } catch (error) {
            console.log('⚠️ Update command result:', error.stdout || error.message);
            
            // Alternative: Try overwriting the entire item
            console.log('🔄 Trying alternative upload method...');
            const overwriteCommand = `az rest --method PATCH --url "https://api.fabric.microsoft.com/v1/workspaces/${workspaceId}/items/${notebookId}" --resource "https://api.fabric.microsoft.com" --body @${tempFile}`;
            
            try {
                const overwriteResult = execSync(overwriteCommand, { encoding: 'utf8' });
                console.log('✅ Notebook overwritten successfully!');
                console.log('Response:', overwriteResult);
            } catch (overwriteError) {
                console.log('📝 Alternative method result:', overwriteError.stdout || overwriteError.message);
            }
        }
        
        // Clean up
        if (fs.existsSync(tempFile)) {
            fs.unlinkSync(tempFile);
        }
        
        console.log('🎉 Deployment process completed!');
        console.log('');
        console.log('📋 Next steps:');
        console.log('1. Open Fabric Portal: https://app.fabric.microsoft.com/');
        console.log(`2. Navigate to workspace: ${workspaceId}`);
        console.log('3. Open the "LakehouseDataPopulation" notebook');
        console.log('4. Ensure MCPDemoLakehouse is set as the default lakehouse');
        console.log('5. Run all cells to populate the data');
        
    } catch (error) {
        console.error('❌ Error deploying notebook:', error.message);
        
        // Provide manual instructions
        console.log('');
        console.log('📋 Manual deployment instructions:');
        console.log('1. Open Fabric Portal: https://app.fabric.microsoft.com/');
        console.log('2. Navigate to your workspace');
        console.log('3. Open the existing "LakehouseDataPopulation" notebook');
        console.log('4. Copy the content from fabric-lakehouse-data-population.ipynb');
        console.log('5. Paste it into the Fabric notebook');
        console.log('6. Set MCPDemoLakehouse as the default lakehouse');
        console.log('7. Run all cells to populate the data');
    }
}

// Run the deployment
deployNotebookToFabric();
