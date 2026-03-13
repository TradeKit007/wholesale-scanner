const axios = require('axios');
const aws4 = require('aws4');

const CONFIG = {
    lwa: {
        clientId: process.env.LWA_APP_ID,
        clientSecret: process.env.LWA_CLIENT_SECRET,
        refreshToken: process.env.LWA_REFRESH_TOKEN
    },
    aws: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
        region: process.env.AWS_REGION || 'us-east-1'
    },
    marketplaceId: process.env.MARKETPLACE_ID || 'ATVPDKIKX0DER',
    keepaApiKey: process.env.KEEPA_API_KEY || ''
};

const MIN_VALID_UPC_LENGTHS = [8, 12, 13, 14];

/**
 * Normalize UPC: Excel silently drops leading zeros (stores as number).
 * e.g. "086486024761" (12-digit UPC-A) → Excel saves as 86486024761 (11 digits)
 * Fix: pad 11→12 and 7→8 with leading zero to restore the original barcode.
 */
function normalizeUpc(raw) {
    let digits = String(raw || '').replace(/[\s-]/g, '');
    if (digits.length === 11) digits = '0' + digits; // UPC-A (12) with dropped leading 0
    if (digits.length === 7) digits = '0' + digits; // UPC-E / EAN-8 with dropped leading 0
    return digits;
}

async function getAccessToken() {
    const response = await axios.post('https://api.amazon.com/auth/o2/token', {
        grant_type: 'refresh_token',
        refresh_token: CONFIG.lwa.refreshToken,
        client_id: CONFIG.lwa.clientId,
        client_secret: CONFIG.lwa.clientSecret
    }, { headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' } });
    return response.data.access_token;
}

async function callSpApi(method, path, body = null, accessToken, retries = 5) {
    const host = 'sellingpartnerapi-na.amazon.com';
    let delay = 3000; // Start at 3s (pricing endpoint allows 0.5 req/s = 2s minimum)

    for (let attempt = 1; attempt <= retries + 1; attempt++) {
        const opts = {
            host, path, method,
            headers: {
                'x-amz-access-token': accessToken,
                'Accept': 'application/json'
            },
            service: 'execute-api', region: CONFIG.aws.region,
            body: body ? JSON.stringify(body) : undefined
        };
        if (body) {
            opts.headers['Content-Type'] = 'application/json';
        }
        aws4.sign(opts, CONFIG.aws);

        try {
            const res = await axios({
                url: `https://${host}${path}`,
                method,
                headers: opts.headers,
                data: opts.body,
                validateStatus: () => true
            });

            if (res.status === 429 && attempt <= retries) {
                // Respect Retry-After header if provided, otherwise exponential backoff
                const retryAfterSec = parseFloat(res.headers?.['retry-after']) || 0;
                const waitMs = retryAfterSec > 0 ? retryAfterSec * 1000 : delay;
                console.warn(`[API] 429 Rate Limit on ${path.split('?')[0]}. Attempt ${attempt}/${retries}. Waiting ${(waitMs / 1000).toFixed(1)}s...`);
                await sleep(waitMs);
                delay = Math.min(delay * 2, 30000); // Exponential backoff, cap at 30s
                continue;
            }

            return res;
        } catch (error) {
            if (attempt <= retries) {
                console.error(`[API] Error on ${path.split('?')[0]}. Attempt ${attempt}/${retries}. ${error.message}`);
                await sleep(delay);
                delay = Math.min(delay * 2, 30000);
                continue;
            }
            throw error;
        }
    }
    return { status: 500, data: { error: 'Max retries reached' } };
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Detects the pack/set multiplier from an Amazon product title.
 * e.g. "Set of 3" → 3, "Pack of 6 Boxes" → 6, "3 Per Case" → 3, "Twin Pack" → 2
 * Returns 1 if no multi-pack detected.
 */
function detectPackMultiplier(title) {
    if (!title) return 1;
    const t = title;
    const patterns = [
        [/(\d+)\s*[-–]\s*pack\b/i, null],           // "3-Pack"
        [/(\d+)\s+pack\b/i, null],                  // "3 Pack"
        [/pack\s+of\s+(\d+)/i, null],               // "Pack of 3"
        [/set\s+of\s+(\d+)/i, null],                // "Set of 3"
        [/bundle\s+of\s+(\d+)/i, null],             // "Bundle of 3"
        [/case\s+of\s+(\d+)/i, null],               // "Case of 6"
        [/box\s+of\s+(\d+)/i, null],                // "Box of 4"
        [/bag\s+of\s+(\d+)/i, null],                // "Bag of 12"
        [/lot\s+of\s+(\d+)/i, null],                // "Lot of 3"
        [/(\d+)\s+per\s+case/i, null],              // "3 Per Case"
        [/(\d+)\s+per\s+pack/i, null],              // "3 Per Pack"
        [/(\d+)\s*[-–]?\s*count\b/i, null],         // "3-Count"
        [/\((\d+)\s*count\)/i, null],               // "(3 Count)"
        [/(\d+)\s*[-–]?\s*ct\b/i, null],            // "3ct", "3-ct"
        [/(\d+)\s*[-–]?\s*piece[s]?\b/i, null],     // "3 Pieces"
        [/(\d+)\s*pk\b/i, null],                   // "3pk"
        [/\((\d+)\s*pack\)/i, null],                // "(3 Pack)"
        [/(\d+)\s+items?\b/i, null],                // "3 Items"
        [/twin\s*pack/i, 2],                     // "Twin Pack" → 2
        [/triple\s*pack/i, 3],                   // "Triple Pack" → 3
        [/quad\s*pack/i, 4],                     // "Quad Pack" → 4
        // NEW PATTERNS
        [/\[(\d+)\s*sets?\]/i, null],
        [/(\d+)\/pk/i, null],
        [/(\d+)\s*[-–]?\s*pairs?/i, null]
    ];
    for (const [rx, fixed] of patterns) {
        const m = t.match(rx);
        if (m) {
            const n = fixed !== null ? fixed : parseInt(m[1]);
            if (n >= 2 && n <= 200) return n;
        }
    }
    const andSetMatch = t.match(/\w[\w\s,'-]{1,40}\s*(?:&\s*\w[\w\s,'-]{1,30})+\s+set\b/i);
    if (andSetMatch) {
        const count = (andSetMatch[0].match(/&/g) || []).length + 1;
        if (count >= 2) return count;
    }
    return 1;
}

async function getAsinAndBsr(upc, token) {
    const path = `/catalog/2022-04-01/items?marketplaceIds=${CONFIG.marketplaceId}&identifiers=${upc}&identifiersType=UPC&includedData=salesRanks,summaries,relationships`;
    const res = await callSpApi('GET', path, null, token);

    if (res.status !== 200 || !res.data.items || res.data.items.length === 0) {
        return { error: `UPC not found in Catalog (HTTP ${res.status})` };
    }

    const item = res.data.items[0];
    const asin = item.asin;
    let title = 'N/A';
    let brand = '';
    if (item.summaries && item.summaries.length > 0) {
        title = item.summaries[0].itemName || 'N/A';
        brand = item.summaries[0].brand || '';
    }

    let main_bsr = null;
    let category = 'Unknown';
    if (item.salesRanks && item.salesRanks.length > 0) {
        const ranks = item.salesRanks[0].displayGroupRanks || [];
        for (const r of ranks) {
            if (r.websiteDisplayGroup) {
                main_bsr = r.rank;
                category = r.title || 'Unknown';
                break;
            }
        }
    }

    let parentAsin = null;
    if (item.relationships && item.relationships.length > 0) {
        const rels = item.relationships[0].relationships || [];
        const parentRel = rels.find(r => r.type === "VARIATION" && r.parentAsins && r.parentAsins.length > 0);
        if (parentRel) {
            parentAsin = parentRel.parentAsins[0];
        }
    }

    let bsr_drop = analyzeBsr(main_bsr, category);

    return { asin, title, brand, main_bsr, category, bsrDrop: bsr_drop, parentAsin };
}

/**
 * AI-driven classification of BSR based on product category.
 * Approximate drop rates: 
 * Hot: > 100 drops/mo
 * Good: 30-100 drops/mo 
 * Slow: < 30 drops/mo
 */
function analyzeBsr(bsr, category) {
    if (!bsr) return "Unknown";

    // Default thresholds
    let hotThreshold = 50000;
    let goodThreshold = 100000;

    // Adjust based on Amazon Category total sizes
    const cat = (category || "").toLowerCase();

    if (cat.includes("home") || cat.includes("kitchen") || cat.includes("clothing") || cat.includes("books")) {
        // Massive categories
        hotThreshold = 100000;
        goodThreshold = 250000;
    } else if (cat.includes("health") || cat.includes("toys") || cat.includes("beauty") || cat.includes("sports")) {
        // Very large categories
        hotThreshold = 75000;
        goodThreshold = 150000;
    } else if (cat.includes("electronics") || cat.includes("office") || cat.includes("pet") || cat.includes("automotive") || cat.includes("grocery")) {
        // Large categories
        hotThreshold = 50000;
        goodThreshold = 100000;
    } else if (cat.includes("video games") || cat.includes("musical") || cat.includes("industrial") || cat.includes("handmade")) {
        // Smaller categories
        hotThreshold = 15000;
        goodThreshold = 40000;
    }

    if (bsr <= hotThreshold) return "🔥 Hot";
    if (bsr <= goodThreshold) return "✅ Good";
    return "🛑 Slow";
}

const AMAZON_SELLER_IDS = [
    'ATVPDKIKX0DER', // Amazon.com
    'A3P5ROKL5A1OLE', // Woot
    'A2L77EE7U53NWQ', // Amazon Warehouse
    'A1PQ2V86QGDX51', // Amazon Fresh
    'A2R2RITDJNW1Q6'  // Amazon (Other/Subsidiary often seen on Beauty/Grocery)
];

function estimateFbaFee(weightG) {
    // Local AWS approximate FBA standard fee calculation (saves 1.5s SP-API call per item!)
    if (!weightG) return 4.52; // Fallback to ~1.0 lb fee if unknown
    const lb = weightG / 453.592;
    if (lb <= 0.25) return 3.22;
    if (lb <= 0.5) return 3.40;
    if (lb <= 1.0) return 3.86;
    if (lb <= 1.5) return 4.38;
    if (lb <= 2.0) return 4.52;
    if (lb <= 2.5) return 4.80;
    if (lb <= 3.0) return 5.08;
    return 5.08 + Math.ceil((lb - 3.0) / 0.5) * 0.28;
}

// Shared pricing payload parser — used by both single and batch pricing endpoints
function parseSingleOfferPayload(payload) {
    const offers = payload.Offers || [];
    const summary = payload.Summary || {};

    const bb_offer = offers.find(o => o.IsBuyBoxWinner);
    let bb_price = null;
    let bb_is_fba = false;
    let bb_is_amazon = false;

    if (bb_offer) {
        const listing = bb_offer.ListingPrice?.Amount || 0;
        const shipping = bb_offer.Shipping?.Amount || 0;
        bb_price = listing + shipping;
        bb_is_fba = !!bb_offer.IsFulfilledByAmazon;
        bb_is_amazon = bb_is_fba && (!bb_offer.SellerFeedbackRating || AMAZON_SELLER_IDS.includes(bb_offer.SellerId));
    } else {
        const buy_box_prices = summary.BuyBoxPrices || [];
        if (buy_box_prices.length > 0) {
            const p = buy_box_prices[0];
            bb_price = (p.ListingPrice?.Amount || 0) + (p.Shipping?.Amount || 0);
        }
    }

    let fba_amz_lowest = null, fbm_lowest = null;
    const fba_offers = [];
    let lowest_fba_is_amazon = false;

    for (const o of offers) {
        const total = (o.ListingPrice?.Amount || 0) + (o.Shipping?.Amount || 0);
        if (o.IsFulfilledByAmazon) {
            fba_offers.push({ total, isAmz: (!o.SellerFeedbackRating || AMAZON_SELLER_IDS.includes(o.SellerId)) });
        } else {
            if (fbm_lowest === null || total < fbm_lowest) fbm_lowest = total;
        }
    }

    if (fba_offers.length > 0) {
        const cheapest = fba_offers.reduce((a, b) => a.total < b.total ? a : b);
        fba_amz_lowest = cheapest.total;
        lowest_fba_is_amazon = cheapest.isAmz;
    }

    let calc_price = 0, strategy = 'None';
    if (bb_price) {
        if (bb_is_fba) {
            calc_price = bb_price;
            strategy = bb_is_amazon ? 'Match BB Amz' : 'Match BB FBA';
        } else {
            const ideal = bb_price * 1.15;
            if (fba_amz_lowest !== null && fba_amz_lowest < ideal) {
                calc_price = fba_amz_lowest;
                strategy = lowest_fba_is_amazon ? 'Match Lowest Amz' : 'Match Lowest FBA';
            } else {
                calc_price = ideal;
                strategy = 'BB FBM + 15%';
            }
        }
    } else {
        if (fba_amz_lowest !== null) {
            calc_price = fba_amz_lowest;
            strategy = lowest_fba_is_amazon ? 'Lowest Amz (No BB)' : 'Lowest FBA (No BB)';
        } else if (fbm_lowest !== null) {
            calc_price = fbm_lowest * 1.15;
            strategy = 'FBM + 15% (No BB)';
        } else {
            return { error: 'Currently unavailable (OOS)' };
        }
    }

    return {
        price: Number(calc_price.toFixed(2)),
        strategy,
        bb_price: bb_price ? Number(bb_price.toFixed(2)) : null,
        offersCount: offers.length
    };
}

// Single-ASIN pricing (kept as fallback for edge cases)
async function getSmartPrice(asin, token) {
    const path = `/products/pricing/v0/items/${asin}/offers?MarketplaceId=${CONFIG.marketplaceId}&ItemCondition=New`;
    const res = await callSpApi('GET', path, null, token);
    if (res.status !== 200 || !res.data.payload) {
        return { error: `Failed to get offers (HTTP ${res.status})` };
    }
    return parseSingleOfferPayload(res.data.payload);
}

// BATCH pricing — up to 20 ASINs per call
// Rate limit: 0.1 req/s = 1 call per 10s. For 20 items: 1 call vs 20 individual calls × 2s = 4× faster.
async function getSmartPriceBatch(asins, token) {
    if (!asins || asins.length === 0) return {};
    const requests = asins.map(asin => ({
        uri: `/products/pricing/v0/items/${asin}/offers`,
        method: 'GET',
        MarketplaceId: CONFIG.marketplaceId,
        ItemCondition: 'New'
    }));
    const res = await callSpApi('POST', '/batches/products/pricing/v0/itemOffers', { requests }, token);
    if (res.status !== 200 || !res.data.responses) {
        console.warn(`[PriceBatch] Failed HTTP ${res.status} — fallback to error for all`);
        return Object.fromEntries(asins.map(a => [a, { error: `Batch pricing failed (HTTP ${res.status})` }]));
    }
    const results = {};
    for (let j = 0; j < asins.length; j++) {
        const asin = asins[j];
        const resp = res.data.responses[j];

        if (!resp) {
            results[asin] = { error: 'No response from batch' };
            continue;
        }

        if (resp.status?.statusCode !== 200 || !resp.body?.payload) {
            results[asin] = { error: `Item pricing HTTP ${resp.status?.statusCode || 'Unknown'}` };
            continue;
        }

        results[asin] = parseSingleOfferPayload(resp.body.payload);
    }
    return results;
}

async function getFees(asin, price, token) {
    const path = `/products/fees/v0/items/${asin}/feesEstimate`;
    const body = {
        FeesEstimateRequest: {
            MarketplaceId: CONFIG.marketplaceId,
            IsAmazonFulfilled: true,
            PriceToEstimateFees: {
                ListingPrice: { CurrencyCode: "USD", Amount: price }
            },
            Identifier: "pipeline_req"
        }
    };

    const res = await callSpApi('POST', path, body, token);
    if (res.status !== 200) {
        return { error: `Failed to get fees (HTTP ${res.status})` };
    }

    const result = res.data.payload?.FeesEstimateResult;
    if (result && result.Status !== "Success") {
        return { error: result.Error?.Message || 'Fee calc error' };
    }

    const totalFee = result?.FeesEstimate?.TotalFeesEstimate?.Amount || 0;
    return { fee: totalFee };
}

/**
 * Calculates the target price, net profit, and ROI based on various pricing strategies.
 * @param {number} cost - The cost of the item.
 * @param {Object} itemPriceInfo - Object containing pricing details (e.g., bb_price, strategy, price).
 * @param {number} totalFee - Total Amazon fees.
 * @param {number} prepFee - Prep fee per unit
 * @returns {Object} { calc_price, strategy, netProfit, roi }
 */
function calcTargetPriceAndRoi(cost, itemPriceInfo, totalFee, prepFee) {
    if (cost <= 0) return { error: 'Invalid cost' };

    let strategy = itemPriceInfo.strategy;
    let calc_price = itemPriceInfo.price;

    // If no price - nothing to calculate
    if (calc_price <= 0) {
        return { error: 'No actionable matching price found' };
    }

    // Calculate profit: (Selling Price) - (Cost) - (Amazon Fee) - (Prep Fee)
    const netProfit = calc_price - cost - totalFee - prepFee;
    let roi = 0;
    if (cost > 0) {
        roi = (netProfit / cost) * 100;
    }

    return { calc_price, strategy, netProfit, roi };
}

// ─────────────────────────────────────────────────────────────
//  KEEPA BATCH
//  KEY: we use stats=0 (no expensive precomputed stats)
//       offers=20 gives FBA fees & buybox price at 0 extra cost.
//  Token cost: EXACTLY 1 per ASIN (was ~34 with stats=30!)
// ─────────────────────────────────────────────────────────────

// Keepa timestamps are minutes since 2011-01-01 00:00 UTC.
const KEEPA_EPOCH_MIN = Math.floor(Date.UTC(2011, 0, 1) / 60000);

function keepaMinutesToMs(km) { return (km + KEEPA_EPOCH_MIN) * 60000; }

function countBsrDrops30(bsrCsv) {
    if (!bsrCsv || bsrCsv.length < 4) return null;
    const cutoffMs = Date.now() - 30 * 24 * 3600 * 1000;

    let drops = 0;
    let lastValidPeak = null;

    for (let i = 0; i < bsrCsv.length - 1; i += 2) {
        const bsr = bsrCsv[i + 1];
        if (bsr < 0) continue; // -1 = no data

        if (keepaMinutesToMs(bsrCsv[i]) < cutoffMs) {
            lastValidPeak = bsr; // Seed the peak before cutoff
            continue;
        }

        if (lastValidPeak !== null) {
            // If BSR worsens (goes up), track the new peak
            if (bsr > lastValidPeak) {
                lastValidPeak = bsr;
            }
            // If BSR improves (goes down), check if it's significant enough
            else {
                const dropAmt = lastValidPeak - bsr;
                const dropPct = dropAmt / lastValidPeak;

                // Noise thresholds: High BSRs fluctuate heavily without sales
                let minReqPct = 0;
                if (lastValidPeak > 600000) minReqPct = 0.06;      // 6% drop required
                else if (lastValidPeak > 300000) minReqPct = 0.04; // 4% drop required
                else if (lastValidPeak > 100000) minReqPct = 0.02; // 2% drop required
                else if (lastValidPeak > 20000) minReqPct = 0.01;  // 1% drop required

                if (dropAmt > 1 && dropPct >= minReqPct) {
                    drops++;
                    lastValidPeak = bsr; // Reset peak after counting the drop
                }
            }
        } else {
            lastValidPeak = bsr;
        }
    }
    return drops;
}

function avgPrice30FromCsv(priceCsv) {
    if (!priceCsv || priceCsv.length < 4) return null;
    const cutoffMs = Date.now() - 30 * 24 * 3600 * 1000;
    let sum = 0, count = 0;
    for (let i = 0; i < priceCsv.length - 1; i += 2) {
        if (keepaMinutesToMs(priceCsv[i]) < cutoffMs) continue;
        const p = priceCsv[i + 1];
        if (p > 0) { sum += p; count++; }
    }
    return count > 0 ? (sum / count / 100).toFixed(2) : null;
}

function avgBsr30FromCsv(bsrCsv) {
    if (!bsrCsv || bsrCsv.length < 4) return null;
    const cutoffMs = Date.now() - 30 * 24 * 3600 * 1000;
    let sum = 0, count = 0;
    for (let i = 0; i < bsrCsv.length - 1; i += 2) {
        if (keepaMinutesToMs(bsrCsv[i]) < cutoffMs) continue;
        const b = bsrCsv[i + 1];
        if (b > 0) { sum += b; count++; }
    }
    return count > 0 ? Math.round(sum / count) : null;
}

async function getKeepaDataBatch(asins) {
    if (!CONFIG.keepaApiKey || asins.length === 0) return {};

    const KEEPA_CHUNK = 20;  // Smaller chunks = less tokens per call
    const results = {};

    for (let i = 0; i < asins.length; i += KEEPA_CHUNK) {
        const chunk = asins.slice(i, i + KEEPA_CHUNK);
        // history=1 is required to get the CSV arrays containing rating, reviews, drops, prices, and BSR!
        // rating=1 gets the rating history.
        const url = `https://api.keepa.com/product?key=${CONFIG.keepaApiKey}&domain=1` +
            `&asin=${chunk.join(',')}&history=1&rating=1`;
        try {
            const res = await axios.get(url, { validateStatus: () => true, timeout: 45000 });

            if (res.status !== 200 || res.data.error) {
                console.warn(`[Keepa Batch] Warning chunk ${i}: status=${res.status} code=${res.data.error?.code}`);

                // If 429 Too Many Requests (Token Limit Reached) -> check exactly how long to wait
                if (res.data.error?.code === 429 || res.status === 429) {
                    const refillInMs = res.data.refillIn || 60000;
                    const tokensLeft = res.data.tokensLeft ?? 'unknown';

                    if (refillInMs > 120000) {
                        console.warn(`[Keepa Batch] ⛔ API Limit (${tokensLeft} tokens). Refill takes ${(refillInMs / 1000).toFixed(1)}s. Too long to wait! Skipping Keepa and falling back to Amazon SP API.`);
                        break; // Exit Keepa immediately, use SP API fallback for all remaining items
                    } else {
                        console.warn(`[Keepa Batch] ⛔ Rate limit hit (${tokensLeft} tokens). Waiting ${(refillInMs / 1000).toFixed(1)}s for tokens to refill...`);
                        await sleep(refillInMs + 1000); // Wait until refill + 1s buffer
                        i -= KEEPA_CHUNK; // Retry same chunk
                    }
                }
                continue;
            }

            const tokensLeft = res.data.tokensLeft ?? 300;
            const consumed = res.data.tokensConsumed ?? 0;
            const products = res.data.products || [];
            console.log(`[Keepa Batch] chunk ${i / KEEPA_CHUNK + 1}: ${products.length} products, tokensLeft=${tokensLeft}, consumed=${consumed}`);

            // ⚠️ If tokens are getting low, pause briefly to let them regenerate, but max 30s to prevent timeout
            if (tokensLeft < 60) {
                const waitSec = Math.min(30, Math.max(10, (100 - tokensLeft) * 1.5));
                console.warn(`[Keepa Batch] ⚠️ Low tokens (${tokensLeft}) — pausing ${waitSec}s to regenerate...`);
                await sleep(waitSec * 1000);
            }

            for (const product of products) {
                const asin = product.asin;
                if (!asin) continue;

                // ── Rating & Reviews ──
                let rating = null, reviews = null;
                if (product.csv?.[16]?.length >= 2) {
                    const h = product.csv[16];
                    const startK = (h.length - 1) % 2 === 1 ? (h.length - 1) : (h.length - 2);
                    for (let j = startK; j >= 1; j -= 2) {
                        if (h[j] > 0) { rating = h[j] / 10; break; }
                    }
                }
                if (product.csv?.[17]?.length >= 2) {
                    const h = product.csv[17];
                    const startK = (h.length - 1) % 2 === 1 ? (h.length - 1) : (h.length - 2);
                    for (let j = startK; j >= 1; j -= 2) {
                        if (h[j] >= 0) { reviews = h[j]; break; }
                    }
                }

                // ── BSR Drops & Avg Price ──
                const drops30 = countBsrDrops30(product.csv?.[3]);
                const avg30Price = avgPrice30FromCsv(product.csv?.[18])
                    || avgPrice30FromCsv(product.csv?.[1])
                    || avgPrice30FromCsv(product.csv?.[0]);

                // ── FBA Fees ──
                const rawPickPack = product.fbaFees?.pickAndPackFee;
                const pickAndPackFee = rawPickPack > 0 ? rawPickPack / 100 : null;
                const rawRefPct = product.referralFeePercent;
                let referralFeeDecimal = null;
                if (rawRefPct > 0) {
                    referralFeeDecimal = rawRefPct > 100 ? rawRefPct / 10000 : rawRefPct / 100;
                }

                let currentBBPrice = null;
                let amazonPrice = null;
                let newPrice = null;

                // csv[18] = Buy Box.
                if (product.csv?.[18]?.length >= 2) {
                    const bbarr = product.csv[18];
                    const startK = (bbarr.length - 1) % 2 === 1 ? (bbarr.length - 1) : (bbarr.length - 2);
                    for (let k = startK; k >= 1; k -= 2) {
                        if (bbarr[k] > 0 && bbarr[k] < 999900) { currentBBPrice = bbarr[k] / 100; break; }
                    }
                }

                // csv[0] = Amazon
                if (product.csv?.[0]?.length >= 2) {
                    const amzarr = product.csv[0];
                    const startK = (amzarr.length - 1) % 2 === 1 ? (amzarr.length - 1) : (amzarr.length - 2);
                    for (let k = startK; k >= 1; k -= 2) {
                        if (amzarr[k] > 0 && amzarr[k] < 999900) { amazonPrice = amzarr[k] / 100; break; }
                    }
                }

                // csv[1] = New Market
                if (product.csv?.[1]?.length >= 2) {
                    const newarr = product.csv[1];
                    const startK = (newarr.length - 1) % 2 === 1 ? (newarr.length - 1) : (newarr.length - 2);
                    for (let k = startK; k >= 1; k -= 2) {
                        if (newarr[k] > 0 && newarr[k] < 999900) { newPrice = newarr[k] / 100; break; }
                    }
                }

                // Fallback: If we didn't pay for Keepa BB tokens, approximate BB with lowest New or Amazon Price
                if (!currentBBPrice && (newPrice || amazonPrice)) {
                    currentBBPrice = newPrice || amazonPrice;
                    if (amazonPrice && amazonPrice < currentBBPrice) currentBBPrice = amazonPrice;
                }

                let currentOffersCount = product.offersSuccessful || 0;
                let pkgQty = product.packageQuantity || 1;
                let avgBsr30 = avgBsr30FromCsv(product.csv?.[3]);

                results[asin] = {
                    rating: rating !== null ? rating.toFixed(1) : 'N/A',
                    reviews: reviews !== null ? reviews : 'N/A',
                    drops30: drops30 !== null ? drops30 : 'N/A',
                    avg30: avg30Price !== null ? avg30Price : 'N/A',
                    packageWeight: product.packageWeight,
                    pkgQty, avgBsr30,
                    pickAndPackFee, referralFeeDecimal,
                    currentBBPrice, amazonPrice, currentOffersCount,
                };
            }
        } catch (e) {
            console.error('[Keepa Batch Error]', e.message);
        }

        if (i + KEEPA_CHUNK < asins.length) await sleep(500);
    }

    return results;
}

// ─────────────────────────────────────────────────────────────

//  SP API BATCH UPC → ASIN  (up to 20 UPCs per call)
//  Returns map: upc → { asin, title, brand, main_bsr, category, bsrDrop, parentAsin }
// ─────────────────────────────────────────────────────────────
async function getAsinBatch(upcs, token) {
    const results = {};
    const BATCH = 20;

    for (let i = 0; i < upcs.length; i += BATCH) {
        const chunk = upcs.slice(i, i + BATCH);
        const identifiers = chunk.join(',');
        const path = `/catalog/2022-04-01/items?marketplaceIds=${CONFIG.marketplaceId}` +
            `&identifiers=${identifiers}&identifiersType=UPC` +
            `&includedData=salesRanks,summaries,relationships,identifiers`;

        const res = await callSpApi('GET', path, null, token);

        if (res.status === 200 && res.data.items) {
            for (const item of res.data.items) {
                const asin = item.asin;

                // Match item back to input UPC via identifiers
                let matchingUpc = null;
                for (const idGroup of item.identifiers || []) {
                    for (const id of idGroup.identifiers || []) {
                        if (chunk.includes(id.identifier)) {
                            matchingUpc = id.identifier;
                            break;
                        }
                    }
                    if (matchingUpc) break;
                }
                if (!matchingUpc) continue;

                let title = 'N/A', brand = '';
                if (item.summaries?.length > 0) {
                    title = item.summaries[0].itemName || 'N/A';
                    brand = item.summaries[0].brand || '';
                }

                let main_bsr = null, category = 'Unknown';
                if (item.salesRanks?.length > 0) {
                    const ranks = item.salesRanks[0].displayGroupRanks || [];
                    for (const r of ranks) {
                        if (r.websiteDisplayGroup) {
                            main_bsr = r.rank;
                            category = r.title || 'Unknown';
                            break;
                        }
                    }
                }

                let parentAsin = null;
                if (item.relationships?.length > 0) {
                    const rels = item.relationships[0].relationships || [];
                    const pRel = rels.find(r => r.type === 'VARIATION' && r.parentAsins?.length > 0);
                    if (pRel) parentAsin = pRel.parentAsins[0];
                }

                // Store ALL matches per UPC → allows multiple Amazon listings per barcode
                if (!results[matchingUpc]) results[matchingUpc] = [];
                results[matchingUpc].push({
                    asin, title, brand, main_bsr, category,
                    bsrDrop: analyzeBsr(main_bsr, category), parentAsin
                });
            }
        } else {
            console.warn(`[SP Batch] UPC batch ${i}–${i + BATCH} returned status ${res.status}`);
        }

        if (i + BATCH < upcs.length) await sleep(500); // 500ms between SP API catalog calls
    }

    return results;
}

/**
 * Process a batch of rows
 * @param {Array} items
 * @param {number} prepFee
 * @param {Array} customBlacklist
 * @param {string} mode - 'full' or 'export_asins'
 */
async function processBatch(items, prepFee = 0.5, customBlacklist = [], mode = 'full') {
    // ── Fetch centralized blacklist ──
    let blacklist = customBlacklist || [];
    try {
        const blUrl = 'https://docs.google.com/spreadsheets/d/1XQz8RSSEnLZ3uih3jnB6ejFsw54LQOR_e-LPQzSlNPk/export?format=csv&gid=0';
        const blRes = await fetch(blUrl);
        if (blRes.ok) {
            const blText = await blRes.text();
            const fetched = blText.split(/[\n,]/).map(b => b.trim().toLowerCase()).filter(b => b.length > 0 && b !== 'бренд');
            blacklist = [...new Set([...blacklist.map(b => b.toLowerCase()), ...fetched])];
        }
    } catch (err) {
        console.warn('[Price Processor] Failed to fetch blacklist:', err.message);
    }

    console.log(`[PriceProcessor] 🚀 BATCH START: ${items.length} items, prepFee=$${prepFee}, blacklist=${blacklist.length}`);
    const t0 = Date.now();

    let token;
    try { token = await getAccessToken(); }
    catch (e) { throw new Error(`Failed to get Amazon LWA Token: ${e.message}`); }

    const profitable = [];
    const problematic = [];

    // ── Validate & Normalize UPCs / ASINs ──
    const validItems = [];
    let initialAsinMap = {};

    for (const item of items) {
        let identifier = '';
        let valid = false;

        const rowBase = { UPC: item.upc || item.asin || '', ItemNumber: item.itemNumber || '', Cost: Number(item.cost) || 0, Description: item.description || '' };

        if (item.asin) {
            // Already an ASIN!
            identifier = item.asin;
            valid = true;
            initialAsinMap[identifier] = [{
                asin: item.asin, title: item.description || 'Pre-mapped ASIN', brand: 'Unknown',
                main_bsr: null, category: 'Unknown', bsrDrop: '⚪ N/A', parentAsin: null
            }];
        } else {
            // It's a UPC
            identifier = normalizeUpc(item.upc);
            if (MIN_VALID_UPC_LENGTHS.includes(identifier.length)) {
                valid = true;
            } else {
                rowBase.Problem = 'Invalid UPC length (' + identifier.length + ' digits: ' + identifier + ')';
                problematic.push(rowBase);
            }
        }

        if (valid) {
            validItems.push({ ...item, _cleanUpc: identifier });
        }
    }

    if (validItems.length === 0) return { profitable, problematic };

    // ════════════════════════════════════════════
    //  PHASE 1 — Batch UPC → ASIN  (20 per call)
    // ════════════════════════════════════════════
    const upcs = validItems.map(i => i._cleanUpc).filter(id => !initialAsinMap[id]);
    const asinMap = { ...initialAsinMap };

    if (upcs.length > 0) {
        console.log(`[Phase 1] Batch UPC→ASIN lookup for ${upcs.length} UPCs...`);
        const spMap = await getAsinBatch(upcs, token);
        Object.assign(asinMap, spMap);
        console.log(`[Phase 1] ✅ Resolved ${Object.keys(spMap).length}/${upcs.length} UPCs in ${((Date.now() - t0) / 1000).toFixed(1)}s`);
    } else {
        console.log(`[Phase 1] All ${validItems.length} items already had ASINs provided. Skipping SP API catalog search.`);
    }

    // ════════════════════════════════════════════
    //  PHASE 1.5 — Individual retry for any UPCs
    //  the batch lookup missed (rate-limit safety net)
    //  If a 20-UPC batch got a 429, those items
    //  get a second chance via individual 1-by-1 calls.
    // ════════════════════════════════════════════
    if (upcs.length > 0) {
        const missedUpcs = validItems
            .map(i => i._cleanUpc)
            .filter(upc => !asinMap[upc] && upcs.includes(upc));

        if (missedUpcs.length > 0) {
            console.log(`[Phase 1.5] ${missedUpcs.length} UPCs missed in batch → retrying in groups of 3...`);
            // Retry in small batches of 3 (much faster than one-by-one, still safe rate-limit-wise)
            const RETRY_CHUNK = 3;
            for (let r = 0; r < missedUpcs.length; r += RETRY_CHUNK) {
                const chunk = missedUpcs.slice(r, r + RETRY_CHUNK);
                await sleep(400); // 400ms between retry-chunks ≈ 1 req/s, well under SP API limit
                const retryResult = await getAsinBatch(chunk, token);
                for (const upc of chunk) {
                    if (retryResult[upc]) {
                        asinMap[upc] = retryResult[upc];
                        console.log(`[Phase 1.5]  ✅ Found: ${upc} → ${retryResult[upc].map(d => d.asin).join(', ')}`);
                    } else {
                        console.log(`[Phase 1.5]  ❌ Not in catalog: ${upc}`);
                    }
                }
            }
            console.log(`[Phase 1.5] Done. Resolved: ${Object.keys(asinMap).length}/${upcs.length} scanned.`);
        }
    }

    // Separate items still not found after retry
    const foundItems = validItems.filter(i => asinMap[i._cleanUpc]);
    for (const i of validItems.filter(i => !asinMap[i._cleanUpc])) {
        problematic.push({
            UPC: i._cleanUpc, ItemNumber: i.itemNumber || '', Cost: Number(i.cost) || 0,
            Description: i.description || '',
            Problem: 'UPC not found in Catalog'
        });
    }


    if (foundItems.length === 0) return { profitable, problematic };

    // ════════════════════════════════════════════
    //  FAST MODE (EXPORT ONLY)
    // ════════════════════════════════════════════
    if (mode === 'export_asins') {
        const asinsExport = [];
        for (const item of foundItems) {
            const asm = asinMap[item._cleanUpc] || [];
            for (const am of asm) {
                asinsExport.push({
                    UPC: item.upc || item._cleanUpc,
                    Cost: item.cost,
                    ItemNumber: item.itemNumber || '',
                    Description: item.description || '',
                    ASIN: am.asin,
                    Title: am.title,
                    Category: am.category,
                    BSR: am.main_bsr || 'N'
                });
            }
        }
        return { profitable: asinsExport, problematic };
    }

    // ════════════════════════════════════════════
    //  PHASE 2 — Keepa BATCH: all ASINs in 1 call
    //            → rating, drops, avg30, FBA fees
    // ════════════════════════════════════════════
    // asinMap: { upc → [asinData, ...] }  (array — supports multiple listings per UPC)
    const asins = [...new Set(foundItems.flatMap(i => asinMap[i._cleanUpc].map(d => d.asin)))];
    console.log(`[Phase 2] Keepa batch for ${asins.length} ASINs...`);
    const keepaMap = CONFIG.keepaApiKey ? await getKeepaDataBatch(asins) : {};
    console.log(`[Phase 2] ✅ Keepa done: ${Object.keys(keepaMap).length} products in ${((Date.now() - t0) / 1000).toFixed(1)}s`);

    // ════════════════════════════════════════════
    //  PHASE 3 — Price: Keepa first (no SP API!)
    //            SP API getSmartPrice only for fallback
    // ════════════════════════════════════════════
    const priceMap = {};
    const needSpPrice = [];

    for (const asin of asins) {
        const k = keepaMap[asin] || {};
        if (k.currentBBPrice && k.currentBBPrice > 0) {
            // ✅ Keepa current Buy Box price — zero SP API calls
            const hasAmz = k.amazonPrice && k.amazonPrice > 0;
            priceMap[asin] = {
                price: k.currentBBPrice,
                bb_price: k.currentBBPrice,
                strategy: hasAmz ? 'Match BB (Amazon)' : 'Match BB FBA',
                offersCount: k.currentOffersCount || 0,
            };
        } else if (k.avg30 && k.avg30 > 0) {
            // ✅ Keepa 30-day average as secondary fallback — still zero SP API calls
            priceMap[asin] = {
                price: k.avg30,
                bb_price: k.avg30,
                strategy: 'Keepa Avg30 (no live BB)',
                offersCount: k.currentOffersCount || 0,
            };
        } else {
            needSpPrice.push(asin); // Neither price available in Keepa → SP API
        }
    }

    if (needSpPrice.length > 0) {
        // BATCH pricing endpoint: 20 ASINs per call, rate 0.1 req/s = 1 call per 10s
        // For 20 items: 1 call (10s) vs individual 20×2s = 40s — 4× faster!
        const PRICE_BATCH = 20;
        console.log(`[Phase 3] SP API batch pricing for ${needSpPrice.length} ASINs (${Math.ceil(needSpPrice.length / PRICE_BATCH)} calls)...`);
        for (let i = 0; i < needSpPrice.length; i += PRICE_BATCH) {
            const chunk = needSpPrice.slice(i, i + PRICE_BATCH);
            const batchResult = await getSmartPriceBatch(chunk, token);
            Object.assign(priceMap, batchResult);
            if (i + PRICE_BATCH < needSpPrice.length) {
                console.log(`[Phase 3] Waiting 11s between pricing batch calls (rate limit 0.1 req/s)...`);
                await sleep(11000);
            }
        }
    }

    const fromKeepa = asins.length - needSpPrice.length;
    const fromSpApi = needSpPrice.length;
    console.log(`[Phase 3] ✅ Prices: ${fromKeepa} from Keepa (0 API calls), ${fromSpApi} from SP API | total ${((Date.now() - t0) / 1000).toFixed(1)}s`);

    // ════════════════════════════════════════════
    //  PHASE 4 — Compute ROI (pure JS, no API)
    //  One foundItem can expand to MULTIPLE rows if UPC has multiple Amazon ASINs
    // ════════════════════════════════════════════
    for (const item of foundItems) {
        const upc = item._cleanUpc;
        const cost = Number(item.cost) || 0;
        const asinDataList = asinMap[upc]; // array: [{asin, title, ...}, ...]

        for (const asinData of asinDataList) {
            const { asin } = asinData;
            const keepa = keepaMap[asin] || {};
            const priceData = priceMap[asin] || { error: 'Price not fetched' };

            let packMultiplier = detectPackMultiplier(asinData.title);
            if (keepa && keepa.pkgQty > 1) {
                packMultiplier = Math.max(packMultiplier, keepa.pkgQty);
            }

            const effectiveCost = cost * packMultiplier;

            let finalBsr = asinData.main_bsr;
            if (!finalBsr || finalBsr <= 0) {
                finalBsr = keepa.avgBsr30 || 'N/A';
            }

            const rowData = {
                UPC: upc, ItemNumber: item.itemNumber || '', Cost: cost,
                Description: item.description || '',
                EffectiveCost: Number(effectiveCost.toFixed(2)),
                VarCount: packMultiplier,
                ASIN: asin, ParentASIN: asinData.parentAsin || null,
                Title: asinData.title, Brand: asinData.brand || 'N/A',
                Category: asinData.category, BSR: finalBsr,
                BSRDrop: asinData.bsrDrop,
                KeepaRating: keepa.rating ?? 'N/A',
                KeepaReviews: keepa.reviews ?? 'N/A',
                KeepaDrops: keepa.drops30 ?? 'N/A',
                KeepaAvg30: keepa.avg30 ?? 'N/A',
            };
            if (packMultiplier > 1) {
                console.log('[Pack] ' + asin + ': "' + (asinData.title || '').substring(0, 60) + '" x' + packMultiplier + ', effectiveCost=$' + effectiveCost.toFixed(2));
            }

            // Brand risk
            rowData.BrandRisk = 'Clear';
            if (blacklist.length > 0 && asinData.brand) {
                const lb = asinData.brand.toLowerCase();
                if (blacklist.some(b => lb.includes(b) || b === lb)) rowData.BrandRisk = 'Restricted';
            }

            // Price
            if (priceData.error) {
                if (priceData.error === 'Currently unavailable (OOS)') {
                    Object.assign(rowData, {
                        BuyBoxPrice: 0, Strategy: 'Currently unavailable',
                        CalcPrice: 0, OffersCount: 0, NetProfit: 0, ROI: 0
                    });
                    profitable.push(rowData);
                } else {
                    rowData.Problem = priceData.error;
                    problematic.push(rowData);
                }
                continue;
            }

            rowData.BuyBoxPrice = priceData.bb_price;
            rowData.Strategy = priceData.strategy;
            rowData.CalcPrice = priceData.price;
            rowData.OffersCount = priceData.offersCount || 0;

            // Local FBA Fees calculation eliminates 1-by-1 SP-API delays!
            let totalFee;
            const referralFee = priceData.price * 0.15; // standard 15% 
            const pickPack = keepa.packageWeight ? estimateFbaFee(keepa.packageWeight) : 4.52;
            totalFee = pickPack + referralFee;
            rowData.FeesSource = 'Estimated (Local)';

            rowData.AmazonFees = Number(totalFee.toFixed(2));

            // ROI — uses effectiveCost (cost x pack multiplier)
            const payout = priceData.price - totalFee;
            const netProfit = payout - effectiveCost - prepFee;
            const roi = effectiveCost > 0 ? (netProfit / effectiveCost) * 100 : 0;

            rowData.PrepFee = prepFee;
            rowData.NetProfit = Number(netProfit.toFixed(2));
            rowData.ROI = Number(roi.toFixed(2));

            if (netProfit > 0) profitable.push(rowData);
            else { rowData.Problem = 'Negative or Zero Net Profit'; problematic.push(rowData); }
        } // end for asinData
    } // end for item


    const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
    console.log(`[PriceProcessor] ✅ DONE in ${elapsed}s — profitable: ${profitable.length}, problematic: ${problematic.length}`);
    return { profitable, problematic };
}

module.exports = {
    processBatch
};
// DEPLOY TRIGGER Fri Mar  6 17:01:22 EET 2026
