/**
 * Used by Github Actions workflows to trigger other workflows.
 */
import GH from './ghclient.js';

// Get workflows from commandline and repo info from environment
const workflowIds = process.argv.slice(2).map(id => `${id}.yml`);
const [owner, repo] = process.env.GITHUB_REPOSITORY.split('/');

if (workflowIds.length === 0) {
    console.error('Usage: node trigger_workflows.js <workflow_id1> [workflow_id2] ...');
    process.exit(1);
}

for (const workflowId of workflowIds) {
    try {
        await GH.req(`/repos/${owner}/${repo}/actions/workflows/${workflowId}/dispatches`, 'POST', {
            ref: "main"
        });
        console.log(`Triggered workflow: ${workflowId}`);
    } catch (error) {
        console.error(`Error triggering workflow "${workflowId}":`, error.message);
        process.exit(1);
    }
}