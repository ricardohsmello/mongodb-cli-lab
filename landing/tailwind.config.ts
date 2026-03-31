import type { Config } from "tailwindcss";
const config: Config = {
  content: ["./app/**/*.{js,ts,jsx,tsx,mdx}"],
  theme: {
    extend: {
      fontFamily: {
        mono: ["JetBrains Mono", "Fira Code", "monospace"],
        sans: ["Inter", "system-ui", "sans-serif"],
      },
      colors: {
        mongo: { green: "#00ED64", dark: "#001E2B", mid: "#023430", card: "#0C2233", border: "#1A3A4A" },
      },
      animation: {
        "blink": "blink 1s step-end infinite",
        "fade-up": "fadeUp 0.6s ease forwards",
        "slide-in": "slideIn 0.5s ease forwards",
      },
      keyframes: {
        blink: { "0%,100%": { opacity: "1" }, "50%": { opacity: "0" } },
        fadeUp: { from: { opacity: "0", transform: "translateY(20px)" }, to: { opacity: "1", transform: "translateY(0)" } },
        slideIn: { from: { opacity: "0", transform: "translateX(-20px)" }, to: { opacity: "1", transform: "translateX(0)" } },
      },
    },
  },
  plugins: [],
};
export default config;
