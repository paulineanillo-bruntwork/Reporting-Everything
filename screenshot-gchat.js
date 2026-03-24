/**
 * Takes a screenshot of the Running Update section and posts it to Google Chat.
 * Runs via GitHub Actions every 4 hours (or manually).
 *
 * Env vars required:
 *   DASHBOARD_URL  - e.g. https://fte-dashboard-production.up.railway.app
 *   APP_PASSWORD   - dashboard login password
 *   GCHAT_WEBHOOK  - Google Chat space webhook URL
 */

const puppeteer = require('puppeteer');
const https = require('https');
const http = require('http');

const DASHBOARD_URL = process.env.DASHBOARD_URL || 'https://fte-dashboard-production.up.railway.app';
const APP_PASSWORD = process.env.APP_PASSWORD || '1234';
const GCHAT_WEBHOOK = process.env.GCHAT_WEBHOOK;

if (!GCHAT_WEBHOOK) {
  console.error('ERROR: GCHAT_WEBHOOK env var is required');
  process.exit(1);
}

async function takeScreenshot() {
  console.log('Launching browser...');
  const browser = await puppeteer.launch({
    headless: 'new',
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
  });

  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 1400, height: 900 });

    // Navigate to login
    console.log('Navigating to dashboard...');
    await page.goto(DASHBOARD_URL + '/login', { waitUntil: 'networkidle2', timeout: 30000 });

    // Login
    console.log('Logging in...');
    await page.type('input[type="password"]', APP_PASSWORD);
    await page.click('button[type="submit"]');
    await page.waitForNavigation({ waitUntil: 'networkidle2', timeout: 60000 });

    // Wait for live data to load (the loading overlay disappears)
    console.log('Waiting for data to load...');
    await page.waitForFunction(function() {
      var overlay = document.getElementById('loadingOverlay');
      return !overlay || overlay.style.display === 'none';
    }, { timeout: 90000 });

    // Extra wait for charts to render
    await new Promise(function(r) { setTimeout(r, 3000); });

    // Screenshot the running update section
    console.log('Taking screenshot of Running Update...');
    var runningSection = await page.$('#runningUpdateSection');
    if (!runningSection) {
      // Fallback: try the whole running update area
      runningSection = await page.$('.running-update');
    }

    var screenshotBuffer;
    if (runningSection) {
      screenshotBuffer = await runningSection.screenshot({ type: 'png' });
    } else {
      // Fallback: screenshot the visible page
      console.log('Running update section not found, taking full page screenshot...');
      screenshotBuffer = await page.screenshot({ type: 'png', fullPage: false });
    }

    console.log('Screenshot taken (' + screenshotBuffer.length + ' bytes)');
    return screenshotBuffer;
  } finally {
    await browser.close();
  }
}

function postToGoogleChat(imageBuffer) {
  return new Promise(function(resolve, reject) {
    // Google Chat webhooks support cards with images, but images must be hosted URLs.
    // Since we don't have image hosting, we'll upload to a free image host first,
    // or post a text card with the data. Let's use the simpler text-card approach
    // combined with uploading the image via multipart.

    // Actually, Google Chat webhook supports simple messages and cards.
    // For images, we need a public URL. Let's use imgbb or similar free host.
    // Alternatively, we can post a text summary + a thread with the image.

    // Simplest approach: upload image to imgur (anonymous), then post the URL to GChat.
    uploadToImgur(imageBuffer).then(function(imageUrl) {
      var now = new Date();
      var gmt8 = new Date(now.getTime() + (8 * 60 * 60 * 1000));
      var months = ['January','February','March','April','May','June','July','August','September','October','November','December'];
      var h = gmt8.getUTCHours();
      var ampm = h >= 12 ? 'PM' : 'AM';
      var h12 = h % 12 || 12;
      var timeStr = months[gmt8.getUTCMonth()] + ' ' + gmt8.getUTCDate() + ', ' + gmt8.getUTCFullYear() + ' ' + h12 + ':' + String(gmt8.getUTCMinutes()).padStart(2, '0') + ' ' + ampm + ' (GMT+8)';

      var message = {
        cards: [{
          header: {
            title: 'FTE Dashboard - Running Update',
            subtitle: timeStr,
            imageUrl: 'https://fonts.gstatic.com/s/i/short-term/release/googlesymbols/analytics/default/48px.svg'
          },
          sections: [{
            widgets: [{
              image: { imageUrl: imageUrl }
            }]
          }, {
            widgets: [{
              buttons: [{
                textButton: {
                  text: 'OPEN DASHBOARD',
                  onClick: { openLink: { url: DASHBOARD_URL } }
                }
              }]
            }]
          }]
        }]
      };

      var body = JSON.stringify(message);
      var urlObj = new URL(GCHAT_WEBHOOK);

      var options = {
        hostname: urlObj.hostname,
        path: urlObj.pathname + urlObj.search,
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(body)
        }
      };

      var req = https.request(options, function(res) {
        var data = '';
        res.on('data', function(chunk) { data += chunk; });
        res.on('end', function() {
          if (res.statusCode >= 200 && res.statusCode < 300) {
            console.log('Posted to Google Chat successfully!');
            resolve(data);
          } else {
            reject(new Error('Google Chat error ' + res.statusCode + ': ' + data));
          }
        });
      });
      req.on('error', reject);
      req.write(body);
      req.end();
    }).catch(reject);
  });
}

function uploadToImgur(imageBuffer) {
  return new Promise(function(resolve, reject) {
    var base64 = imageBuffer.toString('base64');
    var body = JSON.stringify({ image: base64, type: 'base64' });

    var options = {
      hostname: 'api.imgur.com',
      path: '/3/image',
      method: 'POST',
      headers: {
        'Authorization': 'Client-ID 546c25a59c58ad7',
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body)
      }
    };

    var req = https.request(options, function(res) {
      var data = '';
      res.on('data', function(chunk) { data += chunk; });
      res.on('end', function() {
        try {
          var json = JSON.parse(data);
          if (json.success && json.data && json.data.link) {
            console.log('Image uploaded to: ' + json.data.link);
            resolve(json.data.link);
          } else {
            reject(new Error('Imgur upload failed: ' + data));
          }
        } catch (e) {
          reject(new Error('Imgur parse error: ' + data));
        }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

async function main() {
  try {
    console.log('=== FTE Dashboard Screenshot → Google Chat ===');
    console.log('Dashboard: ' + DASHBOARD_URL);
    console.log('Time: ' + new Date().toISOString());

    var screenshot = await takeScreenshot();
    await postToGoogleChat(screenshot);

    console.log('Done!');
    process.exit(0);
  } catch (err) {
    console.error('FAILED:', err.message);
    process.exit(1);
  }
}

main();
