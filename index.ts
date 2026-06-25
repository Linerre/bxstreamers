import { WebSocket } from 'undici';
import { writeFileSync } from 'node:fs';
import { createPublicClient, http, fallback } from 'viem';
import { bsc } from 'viem/chains';

const SMART_MONEY = [
  { address: '0xa83b73f5644cde337b61da79589f10ea15548811', alias: 'AntPositions(蚂蚁仓）☄️' },
  { address: '0xd216cf8ee73da8438a3e57dc63703043cfb6e075', alias: '深情丨先信🔶BNB' },
  { address: '0x93c883963af898ab7c41ef9250f9eed71506eb52', alias: '0x3' },
  { address: '0x8ade93ba431a2ce19fc62a9ce97626e69a4a333f', alias: '0xLuck' },
  { address: '0xbf004bff64725914ee36d03b87d6965b0ced4903', alias: '阿峰_Afeng' },
  { address: '0xfb4e4fa492217d8401aa9e893c78707b61923953', alias: '小财神🔶BNB（恶俗企鹅版）' },
  { address: '0xeeefff8ce2710fa490e0fcb794235e873c252d2e', alias: '神龙 🔶 BNB' },
  { address: '0x7a2363a401b2340c7941dd2eeff0196a5078d2e6', alias: '金狗挖掘机 | 0xDavid' },
  { address: '0x55976c6818e4794f3e2e7179eea2cc2202811e11', alias: 'nine lives' },
];

const WS_URL = 'wss://tokyo.bsc.blxrbdn.com/ws';

const BSC_BLOCK_TIME_S = 0.45;

function main() {
  process.loadEnvFile();

  const API_KEY = process.env.BLXR_AUTH_HEADER;
  const INFURA_API_KEY = process.env.INFURA_API_KEY;
  const DURATION_MINUTES = process.env.DURATION_MINUTES;

  if (!API_KEY) throw new Error('BLXR_AUTH_HEADER is not set in .env');

  const DURATION_MS = Number(DURATION_MINUTES ?? 5) * 60_000;

  const rpcClient = createPublicClient({
    chain: bsc,
    transport: fallback([
      http(INFURA_API_KEY ? `https://bsc-mainnet.infura.io/v3/${INFURA_API_KEY}` : ''),
      http('https://bsc-dataseed.binance.org'),
    ]),
  });

  // Lookup structures built once at startup
  const addressToAlias = new Map(SMART_MONEY.map(({ address, alias }) => [address, alias]));
  const smartMoneySet = new Set(addressToAlias.keys());

  // Per-address match tracking
  type MatchedTx = { txHash: string; timestamp: number };
  const matchesByAddress = new Map<string, Map<string, MatchedTx>>();
  for (const { address } of SMART_MONEY) matchesByAddress.set(address, new Map());

  let totalTxs = 0;
  const startTime = Date.now();

  const ws = new WebSocket(WS_URL, { headers: { Authorization: API_KEY } });

  ws.addEventListener('open', () => {
    console.log(`[${ts()}] Connected — streaming newTxs from BSC for ${DURATION_MS / 60_000} min`);
    ws.send(JSON.stringify({
      id: 1,
      method: 'subscribe',
      params: ['newTxs', { include: ['tx_hash', 'tx_contents'] }],
    }));
    setTimeout(shutdown, DURATION_MS);
  });

  ws.addEventListener('message', (event) => {
    let msg: Record<string, unknown>;
    try {
      msg = JSON.parse(event.data as string) as Record<string, unknown>;
    } catch {
      return;
    }

    const result = (msg.params as Record<string, unknown> | undefined)?.result as
      | { txHash?: string; txContents?: { from?: string } }
      | undefined;

    if (!result?.txHash) return;

    totalTxs++;
    const txHash = result.txHash;
    const from = (result.txContents?.from ?? '').toLowerCase();

    if (smartMoneySet.has(from)) {
      matchesByAddress.get(from)!.set(txHash, { txHash, timestamp: Date.now() });
      console.log(`[MATCH] ${addressToAlias.get(from)} | ${txHash}`);
    }
  });

  ws.addEventListener('error', (event) => {
    console.error(`[ERROR]`, event);
  });

  ws.addEventListener('close', (event) => {
    console.log(`[${ts()}] WebSocket closed — code: ${event.code}, reason: ${event.reason}`);
  });

  let shuttingDown = false;
  function shutdown() {
    if (shuttingDown) return;
    shuttingDown = true;
    ws.close();
    shutdownAsync().catch(console.error);
  }

  async function shutdownAsync() {
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    let totalMatches = 0;

    const perAddress = SMART_MONEY.map(({ address, alias }) => {
      const txs = [...matchesByAddress.get(address)!.values()];
      totalMatches += txs.length;
      return { alias, address, matchCount: txs.length, txs };
    });

    const coveragePct =
      totalTxs > 0 ? ((totalMatches / totalTxs) * 100).toFixed(4) + '%' : '0%';

    // Check on-chain nonce delta to detect private transactions
    console.log(`\n[${ts()}] Checking on-chain activity via RPC...`);
    let onChainResults: { alias: string; address: string; onChainTxCount: number }[] = [];
    try {
      const endBlock = await rpcClient.getBlockNumber();
      const blockSpan = BigInt(Math.round(elapsed / BSC_BLOCK_TIME_S));
      const startBlock = endBlock > blockSpan ? endBlock - blockSpan : 0n;

      console.log(`[${ts()}] Block range: ${startBlock} → ${endBlock} (~${blockSpan} blocks)`);

      onChainResults = await Promise.all(
        SMART_MONEY.map(async ({ address, alias }) => {
          const addr = address as `0x${string}`;
          const [nonceStart, nonceEnd] = await Promise.all([
            rpcClient.getTransactionCount({ address: addr, blockNumber: startBlock }),
            rpcClient.getTransactionCount({ address: addr, blockNumber: endBlock }),
          ]);
          const onChainTxCount = nonceEnd - nonceStart;
          if (onChainTxCount > 0) {
            console.log(`[ON-CHAIN] ${alias} sent ${onChainTxCount} tx(s) — not seen in mempool`);
          }
          return { alias, address, onChainTxCount };
        })
      );
    } catch (err) {
      console.error(`[WARN] On-chain nonce check failed:`, err);
    }

    // Merge on-chain counts into per-address report
    const onChainByAddress = new Map(onChainResults.map(r => [r.address, r.onChainTxCount]));
    const perAddressFull = perAddress.map(r => ({
      ...r,
      onChainTxCount: onChainByAddress.get(r.address) ?? null,
    }));

    const report = {
      generatedAt: new Date().toISOString(),
      elapsedSeconds: elapsed,
      totalTxsSeen: totalTxs,
      smartMoneyTxCount: totalMatches,
      coveragePct,
      perAddress: perAddressFull,
    };

    const filename = `coverage-${new Date().toISOString().replace(/[:.]/g, '-')}.json`;
    writeFileSync(filename, JSON.stringify(report, null, 2));

    console.log(`\n=== Results ===`);
    console.log(`Elapsed:      ${elapsed}s`);
    console.log(`Total txs:    ${totalTxs}`);
    console.log(`Smart money:  ${totalMatches} (mempool)`);
    console.log(`Coverage:     ${coveragePct}`);
    for (const r of onChainResults) {
      const mempoolCount = perAddress.find(p => p.address === r.address)?.matchCount ?? 0;
      const tag = r.onChainTxCount > 0 && mempoolCount === 0 ? ' ← private!' : '';
      console.log(`  ${r.alias}: ${mempoolCount} mempool / ${r.onChainTxCount} on-chain${tag}`);
    }
    console.log(`Report:       ${filename}`);

    process.exit(0);
  }

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

function ts() {
  return new Date().toISOString();
}


main();
