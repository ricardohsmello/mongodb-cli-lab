"use client";

import { useState } from "react";

// ─── Icons (inline SVG components) ──────────────────────────────────────────

function IconDocker() {
  return (
    <svg viewBox="0 0 24 24" fill="currentColor" className="w-5 h-5">
      <path d="M13.983 11.078h2.119a.186.186 0 00.186-.185V9.006a.186.186 0 00-.186-.186h-2.119a.185.185 0 00-.185.185v1.888c0 .102.083.185.185.185m-2.954-5.43h2.118a.186.186 0 00.186-.186V3.574a.186.186 0 00-.186-.185h-2.118a.185.185 0 00-.185.185v1.888c0 .102.082.185.185.186m0 2.716h2.118a.187.187 0 00.186-.186V6.29a.186.186 0 00-.186-.185h-2.118a.185.185 0 00-.185.185v1.887c0 .102.082.185.185.186m-2.93 0h2.12a.186.186 0 00.184-.186V6.29a.185.185 0 00-.185-.185H8.1a.185.185 0 00-.185.185v1.887c0 .102.083.185.185.186m-2.964 0h2.119a.186.186 0 00.185-.186V6.29a.185.185 0 00-.185-.185H5.136a.186.186 0 00-.186.185v1.887c0 .102.084.185.186.186m5.893 2.715h2.118a.186.186 0 00.186-.185V9.006a.186.186 0 00-.186-.186h-2.118a.185.185 0 00-.185.185v1.888c0 .102.082.185.185.185m-2.93 0h2.12a.185.185 0 00.184-.185V9.006a.185.185 0 00-.185-.186H8.1a.185.185 0 00-.185.185v1.888c0 .102.083.185.185.185m-2.964 0h2.119a.185.185 0 00.185-.185V9.006a.185.185 0 00-.186-.186H5.136a.186.186 0 00-.186.186v1.887c0 .102.084.185.186.185m-2.92 0h2.12a.185.185 0 00.184-.185V9.006a.185.185 0 00-.184-.186H2.217a.185.185 0 00-.185.185v1.888c0 .102.083.185.185.185M23.763 9.89c-.065-.051-.672-.51-1.954-.51-.338.001-.676.03-1.01.087-.248-1.7-1.653-2.53-1.716-2.566l-.344-.199-.226.327c-.284.438-.49.922-.612 1.43-.23.97-.09 1.882.403 2.661-.595.332-1.55.413-1.744.42H.751a.751.751 0 00-.75.748 11.376 11.376 0 00.692 4.062c.545 1.428 1.355 2.48 2.41 3.124 1.18.723 3.1 1.137 5.275 1.137.983.003 1.963-.086 2.93-.266a12.248 12.248 0 003.823-1.389c.98-.567 1.86-1.288 2.61-2.136 1.252-1.418 1.998-2.997 2.553-4.4h.221c1.372 0 2.215-.549 2.68-1.009.309-.293.55-.65.707-1.046l.098-.288z" />
    </svg>
  );
}

function IconTerminal() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-5 h-5">
      <polyline points="4 17 10 11 4 5" />
      <line x1="12" y1="19" x2="20" y2="19" />
    </svg>
  );
}

function IconStar() {
  return (
    <svg viewBox="0 0 24 24" fill="currentColor" className="w-4 h-4">
      <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z" />
    </svg>
  );
}

function IconGithub() {
  return (
    <svg viewBox="0 0 24 24" fill="currentColor" className="w-5 h-5">
      <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z" />
    </svg>
  );
}

function IconNpm() {
  return (
    <svg viewBox="0 0 24 24" fill="currentColor" className="w-5 h-5">
      <path d="M0 7.334v8h6.666v1.332H12v-1.332h12v-8H0zm6.666 6.664H5.334v-4H3.999v4H1.335V8.667h5.331v5.331zm4 0v1.336H8.001V8.667h5.334v5.332h-2.669v-.001zm12.001 0h-1.33v-4h-1.336v4h-1.335v-4h-1.33v4h-2.671V8.667h8.002v5.331zM10.665 10H12v2.667h-1.335V10z" />
    </svg>
  );
}

function IconCheck() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" className="w-4 h-4">
      <polyline points="20 6 9 17 4 12" />
    </svg>
  );
}

function IconArrow() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-4 h-4">
      <line x1="5" y1="12" x2="19" y2="12" />
      <polyline points="12 5 19 12 12 19" />
    </svg>
  );
}

function IconCopy() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="w-4 h-4">
      <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
      <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
    </svg>
  );
}

// ─── Copy Button ─────────────────────────────────────────────────────────────

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  const copy = () => {
    navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };
  return (
    <button
      onClick={copy}
      className="absolute top-3 right-3 p-2 rounded-lg bg-[#1A3A4A] hover:bg-[#00ED64] hover:text-[#001E2B] transition-all duration-200 text-gray-400"
      title="Copy"
    >
      {copied ? <IconCheck /> : <IconCopy />}
    </button>
  );
}

// ─── Terminal Block ───────────────────────────────────────────────────────────

function TerminalBlock({ lines, title = "Terminal" }: { lines: { type: "cmd" | "out" | "comment"; text: string }[]; title?: string }) {
  const allText = lines.filter((l) => l.type === "cmd").map((l) => l.text).join("\n");
  return (
    <div className="code-block overflow-hidden">
      <div className="flex items-center gap-2 px-4 py-3 border-b border-[#1A3A4A] bg-[#061220]">
        <div className="flex gap-1.5">
          <div className="w-3 h-3 rounded-full bg-[#FF5F57]" />
          <div className="w-3 h-3 rounded-full bg-[#FFBD2E]" />
          <div className="w-3 h-3 rounded-full bg-[#28CA41]" />
        </div>
        <span className="mono text-xs text-gray-500 ml-2">{title}</span>
        <CopyButton text={allText} />
      </div>
      <div className="p-5 space-y-1">
        {lines.map((line, i) => (
          <div key={i} className="mono text-sm leading-relaxed">
            {line.type === "cmd" && (
              <span>
                <span className="text-[#00ED64]">$ </span>
                <span className="text-white">{line.text}</span>
              </span>
            )}
            {line.type === "out" && <span className="text-gray-400 pl-4">{line.text}</span>}
            {line.type === "comment" && <span className="text-gray-600 italic"># {line.text}</span>}
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── Feature Card ─────────────────────────────────────────────────────────────

function FeatureCard({ icon, title, description, badge }: { icon: string; title: string; description: string; badge?: string }) {
  return (
    <div className="card-hover bg-[#0C2233] border border-[#1A3A4A] rounded-2xl p-6 flex flex-col gap-3">
      <div className="flex items-center justify-between">
        <span className="text-3xl">{icon}</span>
        {badge && (
          <span className="text-xs mono px-2 py-1 rounded-full bg-[#00ED64]/10 text-[#00ED64] border border-[#00ED64]/20">
            {badge}
          </span>
        )}
      </div>
      <h3 className="text-white font-semibold text-lg">{title}</h3>
      <p className="text-gray-400 text-sm leading-relaxed">{description}</p>
    </div>
  );
}

// ─── Tab examples ─────────────────────────────────────────────────────────────

const EXAMPLES = {
  standalone: {
    label: "Standalone",
    icon: "🖥️",
    lines: [
      { type: "comment" as const, text: "Simple single-node MongoDB" },
      { type: "cmd" as const, text: "mongodb-cli-lab up --topology standalone --mongodb-version 8.2 --port 28000" },
      { type: "out" as const, text: "✓ Pulling MongoDB 8.2 image..." },
      { type: "out" as const, text: "✓ Starting standalone node on port 28000" },
      { type: "out" as const, text: "✓ Lab is ready! Connect: mongodb://localhost:28000" },
    ],
  },
  "replica-set": {
    label: "Replica Set",
    icon: "🔁",
    lines: [
      { type: "comment" as const, text: "3-node replica set with Search enabled" },
      { type: "cmd" as const, text: "mongodb-cli-lab up --topology replica-set --replicas 3 --search --mongodb-version 8.2 --port 28000" },
      { type: "out" as const, text: "✓ Starting 3-member replica set..." },
      { type: "out" as const, text: "✓ Electing primary node..." },
      { type: "out" as const, text: "✓ MongoDB Search (mongot) enabled" },
      { type: "out" as const, text: "✓ Lab ready! Primary: mongodb://localhost:28000" },
    ],
  },
  sharded: {
    label: "Sharded Cluster",
    icon: "🗂️",
    lines: [
      { type: "comment" as const, text: "2 shards × 3 replicas + sample data" },
      {
        type: "cmd" as const,
        text: "mongodb-cli-lab up --topology sharded --shards 2 --replicas 3 --mongodb-version 8.2 --port 28000 --sample-databases all",
      },
      { type: "out" as const, text: "✓ Starting config servers..." },
      { type: "out" as const, text: "✓ Starting shard-0 (3 nodes)..." },
      { type: "out" as const, text: "✓ Starting shard-1 (3 nodes)..." },
      { type: "out" as const, text: "✓ Loading sample databases..." },
      { type: "out" as const, text: "✓ Cluster ready! mongos: mongodb://localhost:28000" },
    ],
  },
  encryption: {
    label: "Queryable Encryption",
    icon: "🔐",
    lines: [
      { type: "comment" as const, text: "Queryable Encryption on replica-set" },
      { type: "cmd" as const, text: "mongodb-cli-lab up --topology replica-set --replicas 3 --mongodb-version 8.2 --port 28000" },
      { type: "out" as const, text: "✓ Replica set started" },
      { type: "comment" as const, text: "Then run the QE quickstart" },
      { type: "cmd" as const, text: "mongodb-cli-lab qe quickstart" },
      { type: "out" as const, text: "✓ Master key generated" },
      { type: "out" as const, text: "✓ Data keys created" },
      { type: "out" as const, text: "✓ Encrypted collection ready — fields are encrypted at rest!" },
    ],
  },
};

// ─── Command Table ─────────────────────────────────────────────────────────────

const COMMANDS = [
  { cmd: "mongodb-cli-lab", desc: "Open the interactive menu" },
  { cmd: "mongodb-cli-lab up", desc: "Start a lab with a given topology" },
  { cmd: "mongodb-cli-lab status", desc: "Show status of the running lab" },
  { cmd: "mongodb-cli-lab down", desc: "Stop the running lab" },
  { cmd: "mongodb-cli-lab clean", desc: "Remove all containers and volumes" },
  { cmd: "mongodb-cli-lab quickstart", desc: "Run a quickstart script for the topology" },
  { cmd: "mongodb-cli-lab qe quickstart", desc: "Queryable Encryption quickstart" },
  { cmd: "mongodb-cli-lab qe setup", desc: "Create a custom QE demo collection" },
  { cmd: "mongodb-cli-lab qe status", desc: "Show Queryable Encryption lab status" },
];

// ─── Page ─────────────────────────────────────────────────────────────────────

export default function Page() {
  const [activeTab, setActiveTab] = useState<keyof typeof EXAMPLES>("standalone");

  return (
    <div className="min-h-screen bg-[#001E2B] text-white">

      {/* ── Navbar ─────────────────────────────────────────────────────── */}
      <nav className="sticky top-0 z-50 border-b border-[#1A3A4A] bg-[#001E2B]/90 backdrop-blur-md">
        <div className="max-w-6xl mx-auto px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-lg bg-[#00ED64] flex items-center justify-center">
              <span className="text-[#001E2B] font-black text-sm">M</span>
            </div>
            <span className="mono font-semibold text-white text-sm">mongodb-cli-lab</span>
            <span className="mono text-xs text-[#00ED64] border border-[#00ED64]/30 px-2 py-0.5 rounded-full">v2.1.0</span>
          </div>
          <div className="flex items-center gap-3">
            <a
              href="https://www.npmjs.com/package/@ricardohsmello/mongodb-cli-lab"
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-2 text-gray-400 hover:text-white transition-colors text-sm"
            >
              <IconNpm />
              <span className="hidden sm:inline">npm</span>
            </a>
            <a
              href="https://github.com/ricardohsmello/mongodb-cli-lab"
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-2 bg-[#0C2233] border border-[#1A3A4A] hover:border-[#00ED64] px-4 py-2 rounded-lg text-sm transition-all duration-200"
            >
              <IconGithub />
              <span>GitHub</span>
            </a>
          </div>
        </div>
      </nav>

      {/* ── Hero ────────────────────────────────────────────────────────── */}
      <section className="relative overflow-hidden py-24 px-6">
        {/* Background decoration */}
        <div className="absolute inset-0 overflow-hidden pointer-events-none">
          <div className="absolute -top-40 -right-40 w-96 h-96 rounded-full bg-[#00ED64]/5 blur-3xl" />
          <div className="absolute -bottom-40 -left-40 w-96 h-96 rounded-full bg-[#00ED64]/5 blur-3xl" />
          <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[800px] h-[800px] rounded-full bg-[#00ED64]/3 blur-3xl" />
        </div>

        <div className="max-w-6xl mx-auto relative">
          <div className="flex flex-col lg:flex-row items-center gap-16">
            {/* Left: text */}
            <div className="flex-1 text-center lg:text-left">
              <div className="inline-flex items-center gap-2 bg-[#00ED64]/10 border border-[#00ED64]/20 rounded-full px-4 py-2 mb-6">
                <IconDocker />
                <span className="mono text-xs text-[#00ED64]">Docker-powered local labs</span>
              </div>

              <h1 className="text-5xl lg:text-6xl font-black leading-tight mb-6">
                MongoDB labs{" "}
                <span className="gradient-text">up in seconds.</span>
              </h1>

              <p className="text-gray-400 text-lg leading-relaxed mb-8 max-w-xl mx-auto lg:mx-0">
                A Node.js CLI to spin up local MongoDB environments with Docker — standalone, replica set, sharded cluster, Atlas Search, and Queryable Encryption. No Atlas account needed.
              </p>

              <div className="flex flex-col sm:flex-row gap-3 justify-center lg:justify-start mb-10">
                <a
                  href="#install"
                  className="inline-flex items-center justify-center gap-2 bg-[#00ED64] text-[#001E2B] font-bold px-6 py-3 rounded-xl hover:bg-[#00c450] transition-colors"
                >
                  Get Started <IconArrow />
                </a>
                <a
                  href="https://github.com/ricardohsmello/mongodb-cli-lab"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center justify-center gap-2 bg-[#0C2233] border border-[#1A3A4A] hover:border-[#00ED64] px-6 py-3 rounded-xl transition-all duration-200"
                >
                  <IconGithub /> View on GitHub
                </a>
              </div>

              {/* Stats */}
              <div className="flex gap-6 justify-center lg:justify-start">
                {[
                  { label: "npm installs", value: "free" },
                  { label: "topologies", value: "3" },
                  { label: "Docker required", value: "only dep" },
                ].map((s) => (
                  <div key={s.label} className="text-center">
                    <div className="text-[#00ED64] font-bold text-xl">{s.value}</div>
                    <div className="text-gray-500 text-xs mt-0.5">{s.label}</div>
                  </div>
                ))}
              </div>
            </div>

            {/* Right: quick install terminal */}
            <div className="flex-1 w-full max-w-lg">
              <TerminalBlock
                title="Quick Install"
                lines={[
                  { type: "comment", text: "Install globally" },
                  { type: "cmd", text: "npm install -g @ricardohsmello/mongodb-cli-lab" },
                  { type: "out", text: "" },
                  { type: "comment", text: "Launch the interactive menu" },
                  { type: "cmd", text: "mongodb-cli-lab" },
                  { type: "out", text: "" },
                  { type: "out", text: "? What would you like to do?" },
                  { type: "out", text: "  ❯ 🚀  Set up a cluster" },
                  { type: "out", text: "    🔍  Open Search lab" },
                  { type: "out", text: "    🔐  Open Queryable Encryption lab" },
                  { type: "out", text: "    📊  Work with data" },
                ]}
              />
            </div>
          </div>
        </div>
      </section>

      {/* ── Features ────────────────────────────────────────────────────── */}
      <section className="py-20 px-6 border-t border-[#1A3A4A]">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-14">
            <h2 className="text-3xl font-black mb-3">Everything you need to learn MongoDB</h2>
            <p className="text-gray-400 max-w-xl mx-auto">
              From a simple single node to a production-like sharded cluster — spin up any topology with one command.
            </p>
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-5">
            <FeatureCard
              icon="🖥️"
              title="Standalone Node"
              description="The simplest setup — a single MongoDB instance. Perfect for getting started, quick prototyping, or running Search experiments."
            />
            <FeatureCard
              icon="🔁"
              title="Replica Set"
              description="Multi-node replica set with automatic primary election. Supports up to N replicas, built-in failover, and oplog for change streams."
              badge="+ Search"
            />
            <FeatureCard
              icon="🗂️"
              title="Sharded Cluster"
              description="Full sharded topology with config servers, mongos routers, and multiple shards. Includes quickstart scripts for shard key configuration."
            />
            <FeatureCard
              icon="🔍"
              title="MongoDB Search"
              description="Enable Atlas Search (mongot) on standalone or replica set topologies. Comes with sample data and a quickstart to run full-text queries."
              badge="Atlas Search"
            />
            <FeatureCard
              icon="🔐"
              title="Queryable Encryption"
              description="Demo local KMS-based Queryable Encryption on a replica set. Automatically creates data keys, encrypted collections, and runs end-to-end demos."
              badge="QE Lab"
            />
            <FeatureCard
              icon="🎯"
              title="Interactive Menu"
              description="Don't remember the flags? Just run `mongodb-cli-lab` and navigate a full interactive menu to set up, inspect, and tear down your lab."
            />
          </div>
        </div>
      </section>

      {/* ── Install ─────────────────────────────────────────────────────── */}
      <section id="install" className="py-20 px-6 border-t border-[#1A3A4A]">
        <div className="max-w-3xl mx-auto text-center">
          <h2 className="text-3xl font-black mb-3">Get started in 2 steps</h2>
          <p className="text-gray-400 mb-10">Only prerequisite: Docker running on your machine.</p>

          <div className="space-y-4 text-left">
            <div className="flex gap-4 items-start">
              <div className="w-8 h-8 rounded-full bg-[#00ED64] text-[#001E2B] font-black flex items-center justify-center flex-shrink-0 mt-1">
                1
              </div>
              <div className="flex-1">
                <p className="text-white font-medium mb-2">Install globally via npm</p>
                <TerminalBlock
                  title="Install"
                  lines={[{ type: "cmd", text: "npm install -g @ricardohsmello/mongodb-cli-lab" }]}
                />
              </div>
            </div>

            <div className="flex gap-4 items-start">
              <div className="w-8 h-8 rounded-full bg-[#00ED64] text-[#001E2B] font-black flex items-center justify-center flex-shrink-0 mt-1">
                2
              </div>
              <div className="flex-1">
                <p className="text-white font-medium mb-2">Pick a topology and start</p>
                <TerminalBlock
                  title="Start"
                  lines={[
                    { type: "comment", text: "Option A — interactive" },
                    { type: "cmd", text: "mongodb-cli-lab" },
                    { type: "out", text: "" },
                    { type: "comment", text: "Option B — direct command" },
                    { type: "cmd", text: "mongodb-cli-lab up --topology replica-set --replicas 3 --mongodb-version 8.2 --port 28000" },
                  ]}
                />
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* ── Examples / Tabs ─────────────────────────────────────────────── */}
      <section className="py-20 px-6 border-t border-[#1A3A4A]">
        <div className="max-w-4xl mx-auto">
          <div className="text-center mb-10">
            <h2 className="text-3xl font-black mb-3">Pick your topology</h2>
            <p className="text-gray-400">Each one is one command away.</p>
          </div>

          {/* Tabs */}
          <div className="flex flex-wrap gap-2 justify-center mb-6">
            {(Object.keys(EXAMPLES) as (keyof typeof EXAMPLES)[]).map((key) => {
              const ex = EXAMPLES[key];
              const active = activeTab === key;
              return (
                <button
                  key={key}
                  onClick={() => setActiveTab(key)}
                  className={`flex items-center gap-2 px-4 py-2 rounded-xl mono text-sm font-medium transition-all duration-200 ${
                    active
                      ? "tab-active font-bold"
                      : "bg-[#0C2233] border border-[#1A3A4A] text-gray-400 hover:border-[#00ED64] hover:text-white"
                  }`}
                >
                  <span>{ex.icon}</span>
                  <span>{ex.label}</span>
                </button>
              );
            })}
          </div>

          <TerminalBlock title={EXAMPLES[activeTab].label} lines={EXAMPLES[activeTab].lines} />
        </div>
      </section>

      {/* ── Commands Reference ───────────────────────────────────────────── */}
      <section className="py-20 px-6 border-t border-[#1A3A4A]">
        <div className="max-w-4xl mx-auto">
          <div className="text-center mb-10">
            <h2 className="text-3xl font-black mb-3">Command reference</h2>
            <p className="text-gray-400">All the commands at a glance.</p>
          </div>

          <div className="code-block overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-[#1A3A4A] bg-[#061220]">
                    <th className="mono text-left text-[#00ED64] px-6 py-4 font-medium">Command</th>
                    <th className="text-left text-gray-400 px-6 py-4 font-medium">Description</th>
                  </tr>
                </thead>
                <tbody>
                  {COMMANDS.map((row, i) => (
                    <tr key={i} className="border-b border-[#1A3A4A]/50 hover:bg-[#0C2233] transition-colors">
                      <td className="mono text-[#00ED64]/90 px-6 py-4 whitespace-nowrap">{row.cmd}</td>
                      <td className="text-gray-300 px-6 py-4">{row.desc}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Options callout */}
          <div className="mt-6 p-5 bg-[#0C2233] border border-[#1A3A4A] rounded-2xl">
            <p className="text-sm text-gray-400 mb-3">
              <span className="text-white font-semibold">Key flags</span> for <code className="mono text-[#00ED64] text-xs">mongodb-cli-lab up</code>:
            </p>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
              {[
                ["--topology", "standalone | replica-set | sharded"],
                ["--mongodb-version", "e.g. 8.2"],
                ["--port", "e.g. 28000"],
                ["--replicas", "number of replica members"],
                ["--shards", "number of shards (sharded only)"],
                ["--search", "enable MongoDB Search"],
                ["--sample-databases", "all or comma-separated names"],
              ].map(([flag, desc]) => (
                <div key={flag} className="flex items-start gap-3">
                  <span className="text-[#00ED64] mt-0.5">
                    <IconCheck />
                  </span>
                  <span className="mono text-xs text-gray-300">
                    <span className="text-white">{flag}</span>{" "}
                    <span className="text-gray-500">{desc}</span>
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>

      {/* ── Why / Disclaimer ────────────────────────────────────────────── */}
      <section className="py-20 px-6 border-t border-[#1A3A4A]">
        <div className="max-w-4xl mx-auto">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="bg-[#0C2233] border border-[#1A3A4A] rounded-2xl p-6">
              <h3 className="text-white font-bold text-xl mb-4 flex items-center gap-2">
                <span>💡</span> Why this tool?
              </h3>
              <ul className="space-y-3 text-gray-400 text-sm">
                {[
                  "No Atlas account — everything runs locally via Docker",
                  "Learn MongoDB topologies hands-on without cloud costs",
                  "Demo Atlas Search features in a local environment",
                  "Experiment with Queryable Encryption with zero setup",
                  "Perfect for workshops, interviews, and local development",
                ].map((item) => (
                  <li key={item} className="flex items-start gap-2">
                    <span className="text-[#00ED64] mt-0.5 flex-shrink-0">
                      <IconCheck />
                    </span>
                    {item}
                  </li>
                ))}
              </ul>
            </div>

            <div className="bg-[#1a1200] border border-[#3a2a00] rounded-2xl p-6">
              <h3 className="text-yellow-400 font-bold text-xl mb-4 flex items-center gap-2">
                <span>⚠️</span> Disclaimer
              </h3>
              <p className="text-yellow-200/70 text-sm leading-relaxed mb-3">
                This is an <strong className="text-yellow-200">independent community project</strong> — not an official MongoDB product.
              </p>
              <p className="text-yellow-200/70 text-sm leading-relaxed">
                It is intended for <strong className="text-yellow-200">local development, demos, testing, and learning only</strong>. Do not use in production environments.
              </p>
            </div>
          </div>
        </div>
      </section>

      {/* ── CTA ─────────────────────────────────────────────────────────── */}
      <section className="py-24 px-6 border-t border-[#1A3A4A]">
        <div className="max-w-3xl mx-auto text-center">
          <div className="glow-green bg-gradient-to-b from-[#0C2233] to-[#001E2B] border border-[#1A3A4A] rounded-3xl p-12">
            <h2 className="text-4xl font-black mb-4">
              Ready to spin up a{" "}
              <span className="gradient-text">local cluster?</span>
            </h2>
            <p className="text-gray-400 mb-8 text-lg">One command. No account. No cloud. Just Docker.</p>

            <div className="bg-[#001E2B] border border-[#1A3A4A] rounded-2xl p-4 mono text-sm mb-8 text-left relative max-w-lg mx-auto">
              <span className="text-[#00ED64]">$ </span>
              <span className="text-white">npm install -g @ricardohsmello/mongodb-cli-lab</span>
              <CopyButton text="npm install -g @ricardohsmello/mongodb-cli-lab" />
            </div>

            <div className="flex flex-col sm:flex-row gap-3 justify-center">
              <a
                href="https://www.npmjs.com/package/@ricardohsmello/mongodb-cli-lab"
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center justify-center gap-2 bg-[#00ED64] text-[#001E2B] font-bold px-6 py-3 rounded-xl hover:bg-[#00c450] transition-colors"
              >
                <IconNpm /> View on npm
              </a>
              <a
                href="https://github.com/ricardohsmello/mongodb-cli-lab"
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center justify-center gap-2 bg-[#0C2233] border border-[#1A3A4A] hover:border-[#00ED64] px-6 py-3 rounded-xl transition-all duration-200"
              >
                <IconGithub /> Star on GitHub <IconStar />
              </a>
            </div>
          </div>
        </div>
      </section>

      {/* ── Footer ──────────────────────────────────────────────────────── */}
      <footer className="border-t border-[#1A3A4A] py-8 px-6">
        <div className="max-w-6xl mx-auto flex flex-col sm:flex-row items-center justify-between gap-4 text-sm text-gray-500">
          <div className="flex items-center gap-2">
            <div className="w-5 h-5 rounded bg-[#00ED64] flex items-center justify-center">
              <span className="text-[#001E2B] font-black text-xs">M</span>
            </div>
            <span className="mono">mongodb-cli-lab</span>
            <span>·</span>
            <span>MIT License</span>
          </div>
          <div className="flex items-center gap-4">
            <a
              href="https://github.com/ricardohsmello/mongodb-cli-lab"
              target="_blank"
              rel="noopener noreferrer"
              className="hover:text-white transition-colors flex items-center gap-1"
            >
              <IconGithub /> GitHub
            </a>
            <a
              href="https://www.npmjs.com/package/@ricardohsmello/mongodb-cli-lab"
              target="_blank"
              rel="noopener noreferrer"
              className="hover:text-white transition-colors flex items-center gap-1"
            >
              <IconNpm /> npm
            </a>
          </div>
        </div>
      </footer>
    </div>
  );
}
