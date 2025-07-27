// save-and-tag.js
const { execSync, execFileSync } = require('child_process');
const fs = require('fs');

const fileChanged = process.argv[2];
if (!fileChanged) {
    console.error("No file specified.");
    process.exit(1);
}

fs.appendFileSync('debug.log', `[${new Date().toISOString()}] save-and-tag.js triggered with file: ${fileChanged}\n`);

function getLatestTag() {
    try {
        const tags = execSync('git tag', { encoding: 'utf8' })
            .split('\n')
            .filter(tag => /^v\d+\.\d+-beta$/.test(tag))
            .sort((a, b) => {
                const [amaj, apatch] = a.match(/\d+/g).map(Number);
                const [bmaj, bpatch] = b.match(/\d+/g).map(Number);
                return amaj !== bmaj ? amaj - bmaj : apatch - bpatch;
            });

        return tags[tags.length - 1] || 'v1.00-beta';
    } catch (e) {
        return 'v1.00-beta';
    }
}

function incrementTag(tag) {
    const match = tag.match(/^v(\d+)\.(\d+)-beta$/);
    if (!match) return 'v1.01-beta';

    let [_, major, patch] = match;
    let newPatch = String(parseInt(patch, 10) + 1).padStart(2, '0');
    return `v${major}.${newPatch}-beta`;
}

function tagExists(tag) {
    try {
        const tags = execSync('git tag', { encoding: 'utf8' }).split('\n');
        return tags.includes(tag);
    } catch (e) {
        return false;
    }
}

function promptForMessage() {
    try {
        const message = execFileSync('powershell', [
            '-Command',
            `[System.Reflection.Assembly]::LoadWithPartialName('Microsoft.VisualBasic') | Out-Null; ` +
            `[Microsoft.VisualBasic.Interaction]::InputBox('Enter commit message:', 'Commit Message'); exit` // Force exit PowerShell after prompt
        ], { encoding: 'utf8' }).trim();

        return message;
    } catch (e) {
        console.error('Failed to get commit message:', e.message);
        return '';
    }
}

function commitAndTag(version, message, file) {
    if (!message) {
        console.log('No commit message entered. Aborting.');
        return;
    }

    if (tagExists(version)) {
        console.log(`Tag ${version} already exists. Skipping tagging.`);
        return;
    }

    try {
        console.log("Adding file to Git:", file.replace(/\\/g, "/"));
        execSync(`git add .`);
        execSync(`git commit -m "${message}"`);
        execSync(`git tag ${version}`);
        execSync(`git push origin dev --tags`);
        console.log(`Committed and tagged as ${version}`);
    } catch (e) {
        console.error("Git operation failed:", e.message);
    }

    process.exit(0); // Clean exit to close the terminal window
}

const latest = getLatestTag();
const newVersion = incrementTag(latest);
const message = promptForMessage();
commitAndTag(newVersion, message, fileChanged);