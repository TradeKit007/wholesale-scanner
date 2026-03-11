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

async function callSpApi(method, path, body = null, accessToken, retries = 3) {
    const host = 'sellingpartnerapi-na.amazon.com';
    let delay = 1000;

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
                console.warn(`[API] 429 Rate Limit on ${path}. Attempt ${attempt}/${retries}. Retrying in ${delay}ms...`);
                await sleep(delay);
                delay *= 2; // Exponential backoff
                continue;
            }

            return res;
        } catch (error) {
            if (attempt <= retries) {
                console.error(`[API] Error on ${path}. Attempt ${attempt}/${retries}. ${error.message}`);
                await sleep(delay);
                delay *= 2;
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
        [/(\d+)\s*[-–]\s*[Pp]ack\b/, null],           // "3-Pack", "6-Pack"
        [/(\d+)\s+[Pp]ack\b/, null],                  // "3 Pack"
        [/[Pp]ack\s+of\s+(\d+)/, null],               // "Pack of 3" / "Pack of 3 Boxes"
        [/[Ss]et\s+of\s+(\d+)/, null],                // "Set of 3"
        [/[Bb]undle\s+of\s+(\d+)/, null],             // "Bundle of 3"
        [/[Cc]ase\s+of\s+(\d+)/, null],               // "Case of 6"
        [/[Bb]ox\s+of\s+(\d+)/, null],                // "Box of 4"
        [/[Bb]ag\s+of\s+(\d+)/, null],                // "Bag of 12"
        [/[Ll]ot\s+of\s+(\d+)/, null],                // "Lot of 3"
        [/(\d+)\s+[Pp]er\s+[Cc]ase/, null],           // "3 Per Case" ← key fix
        [/(\d+)\s+[Pp]er\s+[Pp]ack/, null],           // "3 Per Pack"
        [/(\d+)\s*[-–]?\s*[Cc]ount\b/, null],         // "3-Count", "3 Count"
        [/\((\d+)\s*[Cc]ount\)/, null],               // "(3 Count)"
        [/(\d+)\s*[-–]?\s*[Cc]t\b/, null],            // "3ct", "3-ct"
        [/(\d+)\s*[-–]?\s*[Pp]iece[s]?\b/, null],    // "3 Pieces", "3-Piece"
        [/(\d+)\s*[Pp]k\b/i, null],                   // "3pk"
        [/\((\d+)\s*[Pp]ack\)/, null],                // "(3 Pack)"
        [/(\d+)\s+[Ii]tems?\b/, null],                // "3 Items"
        [/[Tt]win\s*[Pp]ack/, 2],                     // "Twin Pack" → 2
        [/[Tt]riple\s*[Pp]ack/, 3],                   // "Triple Pack" → 3
        [/[Qq]uad\s*[Pp]ack/, 4],                     // "Quad Pack" → 4
    ];
    for (const [rx, fixed] of patterns) {
        const m = t.match(rx);
        if (m) {
            const n = fixed !== null ? fixed : parseInt(m[1]);
            if (n >= 2 && n <= 200) return n;
        }
    }
    // Pattern: "Noun & Noun Set" — count & before "Set" (e.g. "A & B Set" → 2×)
    const andSetMatch = t.match(/\w[\w\s,'-]{1,40}\s*(?:&\s*\w[\w\s,'-]{1,30})+\s+Set\b/i);
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

async function getSmartPrice(asin, token) {
    const path = `/products/pricing/v0/items/${asin}/offers?MarketplaceId=${CONFIG.marketplaceId}&ItemCondition=New`;
    const res = await callSpApi('GET', path, null, token);

    if (res.status !== 200 || !res.data.payload) {
        return { error: `Failed to get offers (HTTP ${res.status})` };
    }

    const payload = res.data.payload;
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
            const listing = p.ListingPrice?.Amount || 0;
            const shipping = p.Shipping?.Amount || 0;
            bb_price = listing + shipping;
        }
    }

    let fba_amz_lowest = null;
    let fbm_lowest = null;
    const fba_offers = [];
    let lowest_fba_is_amazon = false;

    for (const o of offers) {
        const total = (o.ListingPrice?.Amount || 0) + (o.Shipping?.Amount || 0);
        if (o.IsFulfilledByAmazon) {
            fba_offers.push({ total, isAmz: (!o.SellerFeedbackRating || AMAZON_SELLER_IDS.includes(o.SellerId)) });
        } else {
            if (fbm_lowest === null || total < fbm_lowest) {
                fbm_lowest = total;
            }
        }
    }

    if (fba_offers.length > 0) {
        // Find the cheapest FBA offer
        const cheapest_fba = fba_offers.reduce((prev, curr) => prev.total < curr.total ? prev : curr);
        fba_amz_lowest = cheapest_fba.total;
        lowest_fba_is_amazon = cheapest_fba.isAmz;
    }

    let calc_price = 0;
    let strategy = "None";

    if (bb_price) {
        if (bb_is_fba) {
            calc_price = bb_price;
            strategy = bb_is_amazon ? "Match BB Amz" : "Match BB FBA";
        } else {
            const ideal_fbm_bump = bb_price * 1.15;
            if (fba_amz_lowest !== null && fba_amz_lowest < ideal_fbm_bump) {
                calc_price = fba_amz_lowest;
                strategy = lowest_fba_is_amazon ? "Match Lowest Amz" : "Match Lowest FBA";
            } else {
                calc_price = ideal_fbm_bump;
                strategy = "BB FBM + 15%";
            }
        }
    } else {
        if (fba_amz_lowest !== null) {
            calc_price = fba_amz_lowest;
            strategy = lowest_fba_is_amazon ? "Lowest Amz (No BB)" : "Lowest FBA (No BB)";
        } else if (fbm_lowest !== null) {
            calc_price = fbm_lowest * 1.15;
            strategy = "FBM + 15% (No BB)";
        } else {
            return { error: "Currently unavailable (OOS)" };
        }
    }

    return {
        price: Number(calc_price.toFixed(2)),
        strategy,
        bb_price: bb_price ? Number(bb_price.toFixed(2)) : null,
        offersCount: offers.length
    };
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
    let prevBsr = null;
    for (let i = 0; i < bsrCsv.length - 1; i += 2) {
        if (keepaMinutesToMs(bsrCsv[i]) < cutoffMs) continue;
        const bsr = bsrCsv[i + 1];
        if (bsr < 0) continue; // -1 = no data
        if (prevBsr !== null && bsr < prevBsr) drops++;
        prevBsr = bsr;
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

async function getKeepaDataBatch(asins) {
    if (!CONFIG.keepaApiKey || asins.length === 0) return {};

    const KEEPA_CHUNK = 20;  // Smaller chunks = less tokens per call
    const results = {};

    for (let i = 0; i < asins.length; i += KEEPA_CHUNK) {
        const chunk = asins.slice(i, i + KEEPA_CHUNK);
        // stats=0 → no precomputed stats
        // offers=20 → includes BB price, FBA fees, and success offers WITHOUT costing 5 tokens like buybox=1 does!
        const url = `https://api.keepa.com/product?key=${CONFIG.keepaApiKey}&domain=1` +
            `&asin=${chunk.join(',')}&stats=0&rating=1&offers=20`;
        try {
            const res = await axios.get(url, { validateStatus: () => true, timeout: 45000 });

            if (res.status !== 200 || res.data.error) {
                console.warn(`[Keepa Batch] Warning chunk ${i}: status=${res.status} code=${res.data.error?.code}`);

                // If 429 Too Many Requests (Token Limit Reached) -> check exactly how long to wait
                if (res.data.error?.code === 429 || res.status === 429) {
                    const refillInMs = res.data.refillIn || 60000;
                    const tokensLeft = res.data.tokensLeft ?? 'unknown';

                    if (refillInMs > 30000 || (typeof tokensLeft === 'number' && tokensLeft < -10)) {
                        console.warn(`[Keepa Batch] ⛔ API Limit (${tokensLeft} tokens). Refill takes ${(refillInMs / 1000).toFixed(1)}s. Too long to wait! Skipping Keepa and falling back to Amazon SP API.`);
                        break; // Exit Keepa immediately, use SP API fallback for all remaining items
                    } else {
                        console.warn(`[Keepa Batch] ⛔ Rate limit hit. Waiting ${(refillInMs / 1000).toFixed(1)}s for tokens to refill...`);
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

                // ── Current Buy Box & Amazon Price ──
                let currentBBPrice = null;
                let amazonPrice = null;

                // Without stats=X, we read the LAST value from CSV arrays:
                // csv[18] = Buy Box. The last odd index is the price (integer cents).
                if (product.csv?.[18]?.length >= 2) {
                    const bbarr = product.csv[18];
                    const startK = (bbarr.length - 1) % 2 === 1 ? (bbarr.length - 1) : (bbarr.length - 2);
                    for (let k = startK; k >= 1; k -= 2) {
                        // Some values might be -1 if out of stock
                        if (bbarr[k] > 0 && bbarr[k] < 999900) { // Sanity: < $9999 (>$9999 = likely a Keepa timestamp)
                            currentBBPrice = bbarr[k] / 100; break;
                        }
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

                let currentOffersCount = product.offersSuccessful || 0;

                results[asin] = {
                    rating: rating !== null ? rating.toFixed(1) : 'N/A',
                    reviews: reviews !== null ? reviews : 'N/A',
                    drops30: drops30 !== null ? drops30 : 'N/A',
                    avg30: avg30Price !== null ? avg30Price : 'N/A',
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
                        if (id.identifierType === 'UPC' && chunk.includes(id.identifier)) {
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

        if (i + BATCH < upcs.length) await sleep(1000); // Rate limit: give SP API 1s between calls
    }

    return results;
}

/**
 * Process a batch of rows — OPTIMIZED (batch UPC→ASIN + Keepa batch + parallel prices).
 * @param {Array<{upc: string, cost: number}>} items
 * @param {number} prepFee The cost per unit handling fee
 * @returns {Promise<{profitable: Array, problematic: Array}>}
 */
async function processBatch(items, prepFee = 0.5, customBlacklist = []) {
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

    // ── Validate & Normalize UPCs ──
    const validItems = [];
    for (const item of items) {
        const upc = normalizeUpc(item.upc); // restores leading zeros lost in Excel
        const cost = Number(item.cost) || 0;
        const rowBase = { UPC: upc, ItemNumber: item.itemNumber || '', Cost: cost };
        if (!MIN_VALID_UPC_LENGTHS.includes(upc.length)) {
            rowBase.Problem = 'Invalid UPC length (' + upc.length + ' digits: ' + upc + ')';
            problematic.push(rowBase);
        } else {
            validItems.push({ ...item, _cleanUpc: upc });
        }
    }

    if (validItems.length === 0) return { profitable, problematic };

    // ════════════════════════════════════════════
    //  PHASE 1 — Batch UPC → ASIN  (20 per call)
    // ════════════════════════════════════════════
    const upcs = validItems.map(i => i._cleanUpc);
    console.log(`[Phase 1] Batch UPC→ASIN lookup for ${upcs.length} UPCs...`);
    const asinMap = await getAsinBatch(upcs, token);
    console.log(`[Phase 1] ✅ Resolved ${Object.keys(asinMap).length}/${upcs.length} UPCs in ${((Date.now() - t0) / 1000).toFixed(1)}s`);

    // ════════════════════════════════════════════
    //  PHASE 1.5 — Individual retry for any UPCs
    //  the batch lookup missed (rate-limit safety net)
    //  If a 20-UPC batch got a 429, those items
    //  get a second chance via individual 1-by-1 calls.
    // ════════════════════════════════════════════
    const missedUpcs = validItems
        .map(i => i._cleanUpc)
        .filter(upc => !asinMap[upc]);

    if (missedUpcs.length > 0) {
        console.log(`[Phase 1.5] ${missedUpcs.length} UPCs missed in batch → retrying individually...`);
        for (const upc of missedUpcs) {
            await sleep(600); // Stay well under the 2 req/s SP API rate limit
            const singleResult = await getAsinBatch([upc], token);
            if (singleResult[upc]) {
                asinMap[upc] = singleResult[upc];
                console.log(`[Phase 1.5]  ✅ Found on retry: ${upc} → ${singleResult[upc].map(d => d.asin).join(', ')}`);
            } else {
                console.log(`[Phase 1.5]  ❌ Truly not found: ${upc}`);
            }
        }
        console.log(`[Phase 1.5] Done. Total resolved now: ${Object.keys(asinMap).length}/${upcs.length}`);
    }

    // Separate items still not found after retry
    const foundItems = validItems.filter(i => asinMap[i._cleanUpc]);
    for (const i of validItems.filter(i => !asinMap[i._cleanUpc])) {
        problematic.push({
            UPC: i._cleanUpc, ItemNumber: i.itemNumber || '', Cost: Number(i.cost) || 0,
            Problem: 'UPC not found in Catalog'
        });
    }


    if (foundItems.length === 0) return { profitable, problematic };

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
            // ✅ Use Keepa Buy Box — zero SP API calls
            const hasAmz = k.amazonPrice && k.amazonPrice > 0;
            priceMap[asin] = {
                price: k.currentBBPrice,
                bb_price: k.currentBBPrice,
                strategy: hasAmz ? 'Match BB (Amazon)' : 'Match BB FBA',
                offersCount: k.currentOffersCount || 0,
            };
        } else {
            needSpPrice.push(asin); // No Keepa price → SP API fallback
        }
    }

    if (needSpPrice.length > 0) {
        const PARALLEL = 5;
        console.log(`[Phase 3] Keepa missing price for ${needSpPrice.length}/${asins.length} ASINs → SP API fallback...`);
        for (let i = 0; i < needSpPrice.length; i += PARALLEL) {
            const chunk = needSpPrice.slice(i, i + PARALLEL);
            const results = await Promise.all(chunk.map(a => getSmartPrice(a, token).catch(e => ({ error: e.message }))));
            chunk.forEach((a, idx) => { priceMap[a] = results[idx]; });
            if (i + PARALLEL < needSpPrice.length) await sleep(300);
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

            // Pack/Set Multiplier
            const packMultiplier = detectPackMultiplier(asinData.title);
            const effectiveCost = cost * packMultiplier;

            const rowData = {
                UPC: upc, ItemNumber: item.itemNumber || '',
                Cost: cost,
                EffectiveCost: Number(effectiveCost.toFixed(2)),
                VarCount: packMultiplier,
                ASIN: asin, ParentASIN: asinData.parentAsin || null,
                Title: asinData.title, Brand: asinData.brand || 'N/A',
                Category: asinData.category, BSR: asinData.main_bsr || 'N',
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

            // FBA Fees: Keepa first, SP API fallback
            let totalFee;
            if (keepa.pickAndPackFee != null && keepa.referralFeeDecimal != null) {
                const referralFee = keepa.referralFeeDecimal * priceData.price;
                totalFee = keepa.pickAndPackFee + referralFee;
                rowData.FeesSource = 'Keepa';
                console.log('[Fees/Keepa] ' + asin + ': pick&pack=$' + keepa.pickAndPackFee.toFixed(2) + ', referral=' + (keepa.referralFeeDecimal * 100).toFixed(2) + '%=$' + referralFee.toFixed(2) + ', total=$' + totalFee.toFixed(2));
            } else {
                const feeData = await getFees(asin, priceData.price, token);
                if (feeData.error) { rowData.Problem = feeData.error; problematic.push(rowData); continue; }
                totalFee = feeData.fee;
                rowData.FeesSource = 'SP API';
            }

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
