/**
 * Polls GitHub Actions workflows and waits for completion.
 */
import { Octokit } from '@octokit/rest';

if (!process.env.GH_AUTH_TOKEN) {
    console.error('Github auth token is not set');
    process.exit(1);
}
const octokit = new Octokit({
    auth: process.env.GH_AUTH_TOKEN,
});

// Default timeout 1 hour
const POLL_TIMEOUT_SEC = parseInt(process.env.POLL_TIMEOUT_SEC || 3600);

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

const workflowNames = process.argv.slice(2);
if (workflowNames.length === 0) {
    console.error('Usage: node poll_workflow.js <workflow_name1> [workflow_name2] ...');
    process.exit(1);
}

const [owner, repo] = process.env.GITHUB_REPOSITORY.split('/');
console.log(`Polling workflows: ${workflowNames.join(', ')}`);

// Poll every pollInterval seconds to see if workflows are done
const pollInterval = 15000; // 15 seconds
const timeoutSeconds = POLL_TIMEOUT_SEC;
const maxTime = timeoutSeconds * 1000;
const maxAttempts = maxTime / pollInterval;
for (let attempt = 0; attempt < maxAttempts; attempt++) {
    await sleep(pollInterval);

    // Use API to check each workflow for completion
    let allSuccess = true;
    for (const workflowName of workflowNames) {
        try {
            const { data } = await octokit.actions.listWorkflowRuns({
                owner,
                repo,
                workflow_id: `${workflowName}.yml`,
                per_page: 1
            });

            const run = data?.workflow_runs?.[0];
            let result = 'pending';
            if (run?.status === 'completed') {
                result = (run.conclusion === 'success') ? 'success' : 'fail';
            }

            if (result === 'success') {
                console.log(`✅ ${workflowName} completed`);
            } else if (result === 'fail') {
                console.log(`❌ ${workflowName} failed`);
                process.exit(1);
            } else {
                allSuccess = false;
            }

        } catch (error) {
            console.error(`Error checking ${workflowName}: ${error.message}`);
            allSuccess = false;
        }
    }

    if (allSuccess) {
        console.log('✅ All workflows completed');
        process.exit(0);
    }

    console.log(`Waiting... (${attempt + 1}/${maxAttempts})`);
}

console.log('❌ Timeout waiting for workflows');
process.exit(1);