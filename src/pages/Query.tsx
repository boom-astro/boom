import { useState, FormEvent } from "react";
import { useNavigate } from "react-router-dom";
import { Select, SelectTrigger, SelectContent, SelectItem, SelectValue } from "@/components/ui/select";
import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";

export default function Query() {
  const navigate = useNavigate();
  const [survey, setSurvey] = useState('ZTF');
  const [objectId, setObjectId] = useState('ZTF25abxeyzt');

  function submit(e?: FormEvent) {
    e?.preventDefault();
    if (!survey || !objectId) return;
    navigate(`/objects/${encodeURIComponent(survey)}/${encodeURIComponent(objectId)}`);
  }

  const gotoExample = () => navigate(`/objects/${encodeURIComponent('ZTF')}/${encodeURIComponent('ZTF25abxeyzt')}`);

  return (
    <div className="px-4 lg:px-6">
      <Card className="max-w-3xl mx-auto">
        <CardHeader>
          <CardTitle>Query Objects</CardTitle>
          <CardDescription>Search the archive by survey and object identifier.</CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={submit} className="grid grid-cols-1 sm:grid-cols-3 gap-3 items-start">
            <div className="sm:col-span-1">
              <label className="text-xs font-medium mb-1 block text-muted-foreground">Survey</label>
              <Select value={survey} onValueChange={(v) => setSurvey(v)}>
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="ZTF">ZTF</SelectItem>
                  <SelectItem value="LSST">LSST</SelectItem>
                  <SelectItem value="Decam">Decam</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="sm:col-span-2">
              <label className="text-xs font-medium mb-1 block text-muted-foreground">Object ID</label>
              <Input value={objectId} onChange={e => setObjectId(e.target.value)} placeholder="Object ID (e.g. ZTF25aagbkaj)" />
            </div>

            <div className="sm:col-span-3 mt-2 text-sm text-muted-foreground">Tip: try the example object if you're exploring the UI.</div>

            <div className="sm:col-span-3 flex justify-end gap-2 mt-1">
              <Button variant="ghost" onClick={gotoExample} type="button">Open example</Button>
              <Button type="submit">Open</Button>
            </div>
          </form>
        </CardContent>
      </Card>
    </div>
  )
}
