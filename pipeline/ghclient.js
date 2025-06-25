/**
 * Basic GitHub API client
 */
import https from 'https';

function req(path, method = 'GET', data = null, timeout = 30000) {
    return new Promise((resolve, reject) => {
        const options = {
            hostname: 'api.github.com',
            port: 443,
            path: path,
            method: method,
            headers: {
                'Authorization': `token ${process.env.REPO_PAT}`,
                'User-Agent': 'GitHub-Actions-Workflow',
                'Accept': 'application/vnd.github.v3+json'
            },
            timeout: timeout
        };

        if (data) {
            options.headers['Content-Type'] = 'application/json';
        }

        const req = https.request(options, (res) => {
            let body = '';
            res.on('data', (chunk) => {
                body += chunk;
            });
            res.on('end', () => {
                if (res.statusCode >= 200 && res.statusCode < 300) {
                    resolve(JSON.parse(body));
                } else {
                    reject(new Error(`HTTP ${res.statusCode}: ${body}`));
                }
            });
        });

        req.on('error', (err) => {
            reject(err);
        });

        req.on('timeout', () => {
            req.destroy();
            reject(new Error(`Request timeout after ${timeout}ms`));
        });

        if (data) {
            req.write(JSON.stringify(data));
        }
        req.end();
    });
}

export default { req }; 