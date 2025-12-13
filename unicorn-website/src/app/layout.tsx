import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "MLB Unicorn Engine",
  description: "Top 50 MLB unicorns with minimalist UI",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className="antialiased">{children}</body>
    </html>
  );
}
