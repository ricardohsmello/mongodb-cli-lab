import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "mongodb-cli-lab — Local MongoDB Labs with Docker",
  description:
    "Spin up local MongoDB labs with Docker in seconds. Standalone, Replica Set, Sharded Cluster, Search, and Queryable Encryption — all from one CLI.",
  openGraph: {
    title: "mongodb-cli-lab",
    description: "Spin up local MongoDB labs with Docker in seconds.",
    type: "website",
  },
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="antialiased">{children}</body>
    </html>
  );
}
