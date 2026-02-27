import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { IconBrandGithub, IconMail, IconMessageCircle } from '@tabler/icons-react';

const channels = [
  {
    icon: IconBrandGithub,
    title: 'Open a GitHub Issue',
    description: 'Found a bug or have a feature request? Open an issue on our public repository and we will triage it as soon as possible.',
    href: 'https://github.com/boom-astro/babamul/issues/new',
    label: 'github.com/boom-astro/babamul/issues',
  },
  {
    icon: IconMessageCircle,
    title: 'GitHub Discussions',
    description: 'Have a question about how something works, or want to share ideas with the community? Start a thread in GitHub Discussions.',
    href: 'https://github.com/boom-astro/babamul/discussions',
    label: 'github.com/boom-astro/babamul/discussions',
  },
  {
    icon: IconMail,
    title: 'Email the Team',
    description: 'Prefer email? Reach us directly at our mailing list and we will get back to you.',
    href: 'mailto:babamul@lists.astro.caltech.edu',
    label: 'babamul@lists.astro.caltech.edu',
  },
];

export default function HelpPage() {
  return (
    <div className="max-w-2xl mx-auto px-4 py-8 flex flex-col gap-8">
      <div>
        <h1 className="text-2xl font-semibold mb-3">Get Help</h1>
        <p className="text-muted-foreground leading-relaxed">
          The Babamul team is committed to supporting the astronomical community. Whether you have
          run into a technical issue, have a question about the data, or want to share feedback,
          we want to hear from you. Choose the channel that works best for you below — we do our
          best to respond promptly and to keep a public record of known issues and resolutions so
          that the whole community can benefit.
        </p>
      </div>

      <div className="flex flex-col gap-4">
        {channels.map(({ icon: Icon, title, description, href, label }) => (
          <a
            key={href}
            href={href}
            target={href.startsWith('mailto') ? undefined : '_blank'}
            rel="noopener noreferrer"
            className="group block no-underline"
          >
            <Card className="transition-colors group-hover:border-primary">
              <CardHeader className="pb-2">
                <CardTitle className="flex items-center gap-2 text-base">
                  <Icon className="size-5 shrink-0" />
                  {title}
                </CardTitle>
              </CardHeader>
              <CardContent className="flex flex-col gap-1">
                <p className="text-sm text-muted-foreground">{description}</p>
                <span className="text-xs text-primary group-hover:underline underline-offset-4 mt-1">
                  {label}
                </span>
              </CardContent>
            </Card>
          </a>
        ))}
      </div>
    </div>
  );
}
