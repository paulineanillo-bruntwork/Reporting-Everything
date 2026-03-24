require('dotenv').config();
const express = require('express');
const path = require('path');
const crypto = require('crypto');
const session = require('express-session');
const { Issuer, generators } = require('openid-client');

const app = express();
const PORT = process.env.PORT || 3000;
const HUBSPOT_KEY = process.env.HUBSPOT_TOKEN;
const HUBSPOT_API = 'https://api.hubapi.com/crm/v3/objects/tickets/search';
const PIPELINES = ['4483329', '3857063', '20565603'];

const KEYCLOAK_URL = process.env.KEYCLOAK_URL;
const KEYCLOAK_REALM = process.env.KEYCLOAK_REALM;
const KEYCLOAK_CLIENT_ID = process.env.KEYCLOAK_CLIENT_ID;
const KEYCLOAK_CLIENT_SECRET = process.env.KEYCLOAK_CLIENT_SECRET;
const APP_URL = process.env.APP_URL || ('http://localhost:' + PORT);
const SESSION_SECRET = process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex');

if (!KEYCLOAK_URL || !KEYCLOAK_REALM || !KEYCLOAK_CLIENT_ID) {
  console.error('Missing required env vars: KEYCLOAK_URL, KEYCLOAK_REALM, KEYCLOAK_CLIENT_ID');
  process.exit(1);
}

// Parse form data and JSON
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

// Session middleware
app.use(session({
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    httpOnly: true,
    sameSite: 'lax',
    secure: APP_URL.startsWith('https'),
    maxAge: 8 * 60 * 60 * 1000 // 8 hours
  }
}));

// OIDC client — initialised before server starts
var oidcClient;

async function initOIDC() {
  var issuerUrl = KEYCLOAK_URL + '/realms/' + KEYCLOAK_REALM;
  var issuer = await Issuer.discover(issuerUrl);
  oidcClient = new issuer.Client({
    client_id: KEYCLOAK_CLIENT_ID,
    client_secret: KEYCLOAK_CLIENT_SECRET,
    redirect_uris: [APP_URL + '/auth/callback'],
    response_types: ['code']
  });
  console.log('OIDC client initialised for issuer:', issuer.issuer);
}

// Block search engines
app.get('/robots.txt', function(req, res) {
  res.type('text/plain');
  res.send('User-agent: *\nDisallow: /\n');
});

// Redirect legacy /login to /auth/login
app.get('/login', function(req, res) {
  res.redirect('/auth/login');
});

// Redirect to Keycloak login
app.get('/auth/login', function(req, res) {
  if (req.session.user) return res.redirect('/');
  var state = generators.state();
  var nonce = generators.nonce();
  req.session.oidcState = state;
  req.session.oidcNonce = nonce;
  var url = oidcClient.authorizationUrl({
    scope: 'openid profile email',
    state: state,
    nonce: nonce
  });
  req.session.save(function(err) {
    if (err) {
      console.error('Session save error:', err);
      return res.status(500).send('Session error');
    }
    res.redirect(url);
  });
});

// Keycloak callback — exchange code for tokens
app.get('/auth/callback', async function(req, res) {
  try {
    var params = oidcClient.callbackParams(req);
    var tokenSet = await oidcClient.callback(
      APP_URL + '/auth/callback',
      params,
      { state: req.session.oidcState, nonce: req.session.oidcNonce }
    );
    var claims = tokenSet.claims();
    req.session.user = {
      sub: claims.sub,
      name: claims.name || claims.preferred_username,
      email: claims.email
    };
    req.session.idToken = tokenSet.id_token;
    delete req.session.oidcState;
    delete req.session.oidcNonce;
    res.redirect('/');
  } catch (err) {
    console.error('OIDC callback error:', err.message);
    res.redirect('/auth/login?error=auth_failed');
  }
});

// Logout — destroy session and redirect to Keycloak end-session
app.get('/logout', function(req, res) {
  var idToken = req.session.idToken;
  req.session.destroy(function() {
    res.clearCookie('connect.sid');
    try {
      var logoutUrl = oidcClient.endSessionUrl({
        post_logout_redirect_uri: APP_URL,
        id_token_hint: idToken
      });
      res.redirect(logoutUrl);
    } catch (e) {
      res.redirect(APP_URL);
    }
  });
});

// Auth middleware - protect everything except auth routes and robots.txt
var PUBLIC_PATHS = ['/auth/login', '/auth/callback', '/login', '/robots.txt'];
app.use(function(req, res, next) {
  if (PUBLIC_PATHS.indexOf(req.path) !== -1) return next();
  if (!req.session.user) {
    if (req.path.startsWith('/api/')) return res.status(401).json({ error: 'Unauthorized' });
    return res.redirect('/auth/login');
  }
  next();
});

// Serve static files (only after auth check)
app.use(express.static(path.join(__dirname)));

async function hubspotSearch(body) {
  var url = HUBSPOT_API;
  var headers = { 'Content-Type': 'application/json' };

  if (HUBSPOT_KEY && HUBSPOT_KEY.startsWith('pat-')) {
    headers['Authorization'] = 'Bearer ' + HUBSPOT_KEY;
  } else {
    url = HUBSPOT_API + '?hapikey=' + HUBSPOT_KEY;
  }

  const res = await fetch(url, {
    method: 'POST',
    headers: headers,
    body: JSON.stringify(body)
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error('HubSpot API error ' + res.status + ': ' + text);
  }
  return res.json();
}

async function fetchAllPages(baseBody) {
  var results = [];
  var after = undefined;
  var hasMore = true;
  while (hasMore) {
    var body = Object.assign({}, baseBody, { limit: 200 });
    if (after) body.after = after;
    var data = await hubspotSearch(body);
    results = results.concat(data.results || []);
    if (data.paging && data.paging.next && data.paging.next.after) {
      after = data.paging.next.after;
    } else {
      hasMore = false;
    }
  }
  return results;
}

app.get('/api/tickets', async function(req, res) {
  try {
    var cutoff = Date.now() - (120 * 24 * 60 * 60 * 1000);
    var cutoffStr = String(cutoff);

    var createdResults = await fetchAllPages({
      filterGroups: [{
        filters: [
          { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
          { propertyName: 'createdate', operator: 'GTE', value: cutoffStr }
        ]
      }],
      properties: ['createdate', 'assignment_type', 'hs_pipeline'],
      sorts: [{ propertyName: 'createdate', direction: 'DESCENDING' }]
    });

    var offboardResults = await fetchAllPages({
      filterGroups: [{
        filters: [
          { propertyName: 'hs_pipeline', operator: 'IN', values: PIPELINES },
          { propertyName: 'offboarding_date', operator: 'GTE', value: cutoffStr }
        ]
      }],
      properties: ['offboarding_date', 'assignment_type', 'hs_pipeline']
    });

    var raw = createdResults.map(function(r) {
      return {
        d: r.properties.createdate,
        t: r.properties.assignment_type || 'Unknown',
        p: r.properties.hs_pipeline
      };
    });

    var offboard = offboardResults.map(function(r) {
      return {
        o: r.properties.offboarding_date,
        t: r.properties.assignment_type || 'Unknown'
      };
    });

    var now = new Date();
    var pht = new Date(now.getTime() + (8 * 60 * 60 * 1000));
    var months = ['January','February','March','April','May','June','July','August','September','October','November','December'];
    var h = pht.getUTCHours();
    var ampm = h >= 12 ? 'PM' : 'AM';
    var h12 = h % 12 || 12;
    var timestamp = months[pht.getUTCMonth()] + ' ' + pht.getUTCDate() + ', ' + pht.getUTCFullYear() + ' ' + h12 + ':' + String(pht.getUTCMinutes()).padStart(2, '0') + ' ' + ampm + ' (GMT+8)';

    res.json({
      raw: raw,
      offboard: offboard,
      timestamp: timestamp,
      counts: { created: raw.length, offboarded: offboard.length }
    });
  } catch (err) {
    console.error('API Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

initOIDC().then(function() {
  app.listen(PORT, function() {
    console.log('FTE Dashboard server running at http://localhost:' + PORT);
  });
}).catch(function(err) {
  console.error('Failed to initialise OIDC:', err.message);
  process.exit(1);
});
// deployed Mon Mar 23 08:33:54 MPST 2026
