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

async function getKeepaData(asin) {
    if (!CONFIG.keepaApiKey) return null;

    try {
        const url = `https://api.keepa.com/product?key=${CONFIG.keepaApiKey}&domain=1&asin=${asin}&stats=30&rating=1&buybox=1`;
        const res = await axios.get(url, { validateStatus: () => true });

        if (res.status !== 200 || res.data.error) {
            console.warn(`[Keepa API Warning] ASIN: ${asin}. Code: ${res.data.error?.code}`);
            return null;
        }

        const product = res.data.products && res.data.products[0];
        if (!product) return null;

        let rating = null;
        let drops30 = null;
        let avg30Price = null;

        if (product.stats) {
            drops30 = typeof product.stats.salesRankDrops30 === 'number' ? product.stats.salesRankDrops30 : null;
            if (product.stats.avg30) {
                const bb = product.stats.avg30[18];
                const newP = product.stats.avg30[1];
                const amz = product.stats.avg30[0];
                let val = -1;
                if (bb >= 0) val = bb;
                else if (newP >= 0) val = newP;
                else if (amz >= 0) val = amz;

                if (val > 0) avg30Price = (val / 100).toFixed(2);
            }
        }

        if (product.csv && product.csv[16] && product.csv[16].length >= 2) {
            const history = product.csv[16];
            for (let i = history.length - 1; i >= 1; i -= 2) {
                if (history[i] > 0) {
                    rating = history[i] / 10;
                    break;
                }
            }
        }

        let reviews = null;
        if (product.csv && product.csv[17] && product.csv[17].length >= 2) {
            const history = product.csv[17];
            for (let i = history.length - 1; i >= 1; i -= 2) {
                if (history[i] >= 0) {
                    reviews = history[i];
                    break;
                }
            }
        }

        return {
            rating: rating !== null ? rating.toFixed(1) : 'N/A',
            reviews: reviews !== null ? reviews : 'N/A',
            drops30: drops30 !== null ? drops30 : 'N/A',
            avg30: avg30Price !== null ? avg30Price : 'N/A'
        };
    } catch (e) {
        console.error('[Keepa Error]', e.message);
        return null;
    }
}

/**
 * Process a batch of rows.
 * @param {Array<{upc: string, cost: number}>} items 
 * @param {number} prepFee The cost per unit handling fee
 * @returns {Promise<{profitable: Array, problematic: Array}>}
 */
async function processBatch(items, prepFee = 0.5, customBlacklist = []) {
    let blacklist = customBlacklist || [];
    try {
        const blUrl = 'https://docs.google.com/spreadsheets/d/1XQz8RSSEnLZ3uih3jnB6ejFsw54LQOR_e-LPQzSlNPk/export?format=csv&gid=0';
        const blRes = await fetch(blUrl);
        if (blRes.ok) {
            const blText = await blRes.text();
            const fetchedBlacklist = blText.split(/[\n,]/).map(b => b.trim().toLowerCase()).filter(b => b.length > 0 && b !== 'бренд');
            blacklist = [...new Set([...blacklist.map(b => b.toLowerCase()), ...fetchedBlacklist])];
        }
    } catch (err) {
        console.warn('[Price Processor] Failed to fetch centralized blacklist:', err.message);
    }

    console.log(`[Price Processor] Starting batch for ${items.length} items with prepFee=${prepFee}... Blacklist items: ${blacklist.length}`);

    let token;
    try {
        token = await getAccessToken();
    } catch (e) {
        throw new Error(`Failed to get Amazon LWA Token: ${e.message}`);
    }

    const profitable = [];
    const problematic = [];

    for (let i = 0; i < items.length; i++) {
        const item = items[i];
        const rawUpc = item.upc || '';
        const itemNumber = item.itemNumber || '';
        const cleanUpc = String(rawUpc).replace(/[\s-]/g, '');
        const cost = Number(item.cost) || 0;

        const rowData = { UPC: cleanUpc, ItemNumber: itemNumber, Cost: cost };

        if (!MIN_VALID_UPC_LENGTHS.includes(cleanUpc.length)) {
            rowData.Problem = 'Invalid UPC length';
            problematic.push(rowData);
            continue;
        }

        console.log(`Processing ${i + 1}/${items.length}: ${cleanUpc}`);

        // 1. ASIN
        const asinData = await getAsinAndBsr(cleanUpc, token);
        if (asinData.error) {
            rowData.Problem = asinData.error;
            problematic.push(rowData);
            await sleep(1000);
            continue;
        }

        rowData.ASIN = asinData.asin;
        rowData.ParentASIN = asinData.parentAsin || null;
        rowData.Title = asinData.title;
        rowData.Brand = asinData.brand || 'N/A';
        rowData.Category = asinData.category;
        rowData.BSR = asinData.main_bsr || 'N';
        rowData.BSRDrop = asinData.bsrDrop;

        rowData.BrandRisk = 'Clear';
        if (blacklist.length > 0 && asinData.brand) {
            const lowBrand = asinData.brand.toLowerCase();
            const isRestricted = blacklist.some(b => lowBrand.includes(b) || b === lowBrand || lowBrand === b);
            if (isRestricted) {
                rowData.BrandRisk = 'Restricted';
            }
        }

        // 1.5 KEEPA (Rating & Drops & Avg30)
        if (CONFIG.keepaApiKey) {
            const keepaData = await getKeepaData(asinData.asin);
            if (keepaData) {
                rowData.KeepaRating = keepaData.rating;
                rowData.KeepaReviews = keepaData.reviews;
                rowData.KeepaDrops = keepaData.drops30;
                rowData.KeepaAvg30 = keepaData.avg30;
            } else {
                rowData.KeepaRating = 'N/A';
                rowData.KeepaReviews = 'N/A';
                rowData.KeepaDrops = 'N/A';
                rowData.KeepaAvg30 = 'N/A';
            }
            await sleep(500); // safety pause for Keepa
        } else {
            rowData.KeepaRating = 'No Key';
            rowData.KeepaReviews = 'No Key';
            rowData.KeepaDrops = 'No Key';
            rowData.KeepaAvg30 = 'No Key';
        }

        // 2. Price
        const priceData = await getSmartPrice(asinData.asin, token);
        if (priceData.error) {
            if (priceData.error === "Currently unavailable (OOS)") {
                // Return in profitable table to indicate opportunity
                rowData.BuyBoxPrice = 0;
                rowData.Strategy = "Currently unavailable";
                rowData.CalcPrice = 0;
                rowData.OffersCount = 0;
                rowData.NetProfit = 0;
                rowData.ROI = 0;
                profitable.push(rowData);
                await sleep(1000);
                continue;
            } else {
                rowData.Problem = priceData.error;
                problematic.push(rowData);
                await sleep(1000);
                continue;
            }
        }

        rowData.BuyBoxPrice = priceData.bb_price;
        rowData.Strategy = priceData.strategy;
        rowData.CalcPrice = priceData.price;
        rowData.OffersCount = priceData.offersCount || 0;

        // 3. Fees
        const feeData = await getFees(asinData.asin, priceData.price, token);
        if (feeData.error) {
            rowData.Problem = feeData.error;
            problematic.push(rowData);
            await sleep(1000);
            continue;
        }

        rowData.AmazonFees = feeData.fee;

        // 4. ROI
        // Here we just use the priceData and feeData that we got above
        const payout = priceData.price - feeData.fee;
        const netProfit = payout - cost - prepFee;
        let roi = 0;
        if (cost > 0) {
            roi = (netProfit / cost) * 100;
        }

        rowData.PrepFee = prepFee;
        rowData.NetProfit = Number(netProfit.toFixed(2));
        rowData.ROI = Number(roi.toFixed(2));

        if (netProfit > 0) {
            profitable.push(rowData);
        } else {
            rowData.Problem = 'Negative or Zero Net Profit';
            problematic.push(rowData);
        }

        await sleep(1000); // SP API Rate Limiting protection
    }

    return { profitable, problematic };
}

module.exports = {
    processBatch
};
