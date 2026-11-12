import { Fragment } from 'react';
import { Card, CardContent, CardTitle } from '@/components/ui/card';
import { ApiObject, HostSummary } from '@/lib/api';

function hostRows(host: HostSummary): [string, string][] {
  const rows: [string, string][] = [];
  if (host.name) rows.push(['Name', host.name]);
  if (host.catalog) rows.push(['Catalog', host.catalog]);
  if (host.z != null) {
    rows.push(['Redshift', host.z_err != null ? `${host.z.toFixed(4)} ± ${host.z_err.toFixed(4)}` : host.z.toFixed(4)]);
  }
  if (host.dist_mpc != null) rows.push(['Distance', `${host.dist_mpc.toFixed(1)} Mpc`]);
  const sep = `${host.sep_arcsec.toFixed(2)}″`;
  rows.push(['Separation', host.sep_kpc != null ? `${sep} (${host.sep_kpc.toFixed(2)} kpc)` : sep]);
  rows.push(['d_DLR', host.d_dlr.toFixed(2)]);
  return rows;
}

export default function HostCard({ data }: { data: ApiObject }) {
  const host = data.best_host as HostSummary | null | undefined;

  return (
    <Card className="@container/card col-span-1">
      <CardContent className="space-y-3">
        <CardTitle className="text-lg">Host</CardTitle>
        {host ? (
          <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-sm">
            {hostRows(host).map(([label, value]) => (
              <Fragment key={label}>
                <dt className="text-muted-foreground">{label}</dt>
                <dd className="font-mono break-all">{value}</dd>
              </Fragment>
            ))}
          </dl>
        ) : (
          <div className="text-sm text-gray-500">No host</div>
        )}
      </CardContent>
    </Card>
  );
}
