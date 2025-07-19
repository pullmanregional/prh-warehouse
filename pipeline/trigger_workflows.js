/**
 * Used by Github Actions workflows to trigger other workflows.
 * 
 * Workflow IDs can be <workflow_name> to use a workflow in the current repo,
 * or <repo>/<workflow_name> to use a different repo.
 */
import { Octokit } from '@octokit/rest';

if (!process.env.GH_AUTH_TOKEN) {
    console.error('Github auth token is not set');
    process.exit(1);
}
const octokit = new Octokit({
    auth: process.env.GH_AUTH_TOKEN,
});

// Get workflows from commandline and repo info from environment
const workflows = process.argv.slice(2).map(id => `${id}.yml`);
const [owner, curRepo] = process.env.GITHUB_REPOSITORY.split('/');

if (workflows.length === 0) {
    console.error('Usage: node trigger_workflows.js <workflow_id1> [workflow_id2] ...');
    process.exit(1);
}

for (const workflow of workflows) {
    // Get repo name from workflow ID if it's in the form <repo>/<workflow_name>
    let repo = curRepo;
    if (workflow.includes('/')) {
        [repo, workflow] = workflow.split('/');
    }

    // Trigger the workflow
    await octokit.actions.createWorkflowDispatch({
        owner: owner,
        repo: repo,
        workflow_id: workflow,
        ref: "main"
    });
    console.log(`Triggered workflow: ${workflow} in ${repo}`);
}