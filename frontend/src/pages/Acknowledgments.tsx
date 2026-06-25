import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { IconNews } from '@tabler/icons-react';
import { ACKNOWLEDGMENTS_TEXT, PAPER_URL } from '@/lib/constants';

export default function Acknowledgments() {
  return (
    <div className="max-w-2xl mx-auto px-4 py-8 flex flex-col gap-8">
      <div>
        <h1 className="text-2xl font-semibold mb-3">Acknowledgments</h1>
        <p className="text-muted-foreground leading-relaxed">
          {ACKNOWLEDGMENTS_TEXT}
        </p>
      </div>

      <div className="flex flex-col gap-4">
        <a
          href={PAPER_URL}
          target="_blank"
          rel="noopener noreferrer"
          className="group block no-underline"
        >
          <Card className="transition-colors group-hover:border-primary">
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2 text-base">
                <IconNews className="size-5 shrink-0" />
                Read our paper
              </CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-1">
              <p className="text-sm text-muted-foreground">
                The Babamul system paper describing the architecture, data pipelines,
                and alert classification framework.
              </p>
              <span className="text-xs text-primary group-hover:underline underline-offset-4 mt-1">
                arxiv.org/abs/2511.00164
              </span>
            </CardContent>
          </Card>
        </a>
      </div>
    </div>
  );
}
