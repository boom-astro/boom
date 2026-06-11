import { useState, useEffect, useRef } from "react";
import mermaid from "mermaid";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import rehypeRaw from "rehype-raw";

const BABAMUL_DOCS_URL = "https://raw.githubusercontent.com/boom-astro/boom/refs/heads/main/docs/babamul.md";

/**
 * Extract the markdown content starting from "## Object appearance in output topics" heading.
 * Removes the heading itself since it's shown in the dialog title.
 */
function extractObjectAppearanceSection(markdown: string): string | null {
  const match = markdown.match(/## Object appearance in output topics\s+([\s\S]+)/);
  return match ? match[1].trim() : null;
}

function MermaidBlock({ code }: { code: string }) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [rendered, setRendered] = useState(false);

  useEffect(() => {
    if (!containerRef.current || rendered) return;

    const renderId = `mermaid-${Math.random().toString(36).slice(2, 8)}`;

    mermaid.initialize({
      startOnLoad: false,
      securityLevel: 'loose',
      theme: 'dark',
      themeVariables: {
        noteBkgColor: '#1a1a1a',
        noteTextColor: '#e0e0e0',
        actorTextColor: '#ffffff',
        labelTextColor: '#ffffff'
      }
    });

    mermaid.render(renderId, code).then(({ svg }) => {
      if (containerRef.current) {
        containerRef.current.innerHTML = svg;
        setRendered(true);
      }
    }).catch((err) => {
      console.error('Failed to render mermaid diagram', err);
      if (containerRef.current) {
        containerRef.current.innerHTML = '<div class="text-red-500">Failed to render diagram</div>';
      }
    });
  }, [code, rendered]);

  return <div ref={containerRef} className="overflow-x-auto bg-black p-4 rounded-lg my-4" aria-label="Mermaid diagram" />;
}

function MarkdownWithMermaid({ content }: { content: string }) {
  return (
    <div className="prose prose-sm dark:prose-invert max-w-none [&>p]:mb-4">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        rehypePlugins={[rehypeRaw]}
        components={{
          h1: ({ children }) => <h1 className="text-2xl font-bold mt-6 mb-4">{children}</h1>,
          h2: ({ children }) => <h2 className="text-xl font-bold mt-5 mb-3">{children}</h2>,
          h3: ({ children }) => <h3 className="text-lg font-semibold mt-4 mb-2">{children}</h3>,
          h4: ({ children }) => <h4 className="text-base font-semibold mt-3 mb-2">{children}</h4>,
          p: ({ children }) => <p className="mb-4">{children}</p>,
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          code({ inline, className, children, ...props }: any) {
            const match = /language-(\w+)/.exec(className || '');
            const language = match ? match[1] : '';

            if (!inline && language === 'mermaid') {
              const code = String(children).replace(/\n$/, '');
              return <MermaidBlock code={code} />;
            }

            return (
              <code className={className} {...props}>
                {children}
              </code>
            );
          },
        }}
      >
        {content}
      </ReactMarkdown>
    </div>
  );
}

export default function KafkaMarkdownDocs() {
  const [objectAppearanceMarkdown, setObjectAppearanceMarkdown] = useState<string>("");

  // Load mermaid diagram from GitHub
  useEffect(() => {
    async function loadDiagram() {
      try {
        const response = await fetch(BABAMUL_DOCS_URL);
        const markdown = await response.text();
        const section = extractObjectAppearanceSection(markdown);
        if (section) {
          console.log('Loaded markdown section:', section.substring(0, 200));
          setObjectAppearanceMarkdown(section);
        } else {
          console.error('Failed to extract section from markdown');
        }
      } catch (err) {
        console.error('Failed to load markdown content:', err);
      }
    }
    loadDiagram();
  }, []);

  return (
    <div>
      {objectAppearanceMarkdown ? (
        <MarkdownWithMermaid content={objectAppearanceMarkdown} />
      ) : (
        <div className="text-sm text-muted-foreground">Loading documentation...</div>
      )}
    </div>
  )
}