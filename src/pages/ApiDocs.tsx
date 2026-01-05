import { useEffect } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { ExternalLink } from "lucide-react";

export default function ApiDocs() {
  const navigate = useNavigate();
  const location = useLocation();

  // Always hit the same-origin proxy; prod should route /api to the backend via nginx
  const docsUrl = "/api/babamul/docs";

  useEffect(() => {
    // Open in new tab
    window.open(docsUrl, '_blank', 'noopener,noreferrer');
    // Navigate back to where we came from, replacing this history entry
    const from = (location.state as { from?: string })?.from || '/';
    navigate(from, { replace: true });
  }, [docsUrl, navigate, location.state]);

  return (
    <div className="px-4 lg:px-6">
      <Card className="max-w-3xl mx-auto">
        <CardHeader>
          <CardTitle>API Documentation</CardTitle>
          <CardDescription>Opening API documentation in a new tab...</CardDescription>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground mb-4">
            The API documentation should open in a new tab.
          </p>
          <Button 
            variant="outline" 
            onClick={() => window.open(docsUrl, '_blank', 'noopener,noreferrer')}
            className="gap-2"
          >
            <ExternalLink className="h-4 w-4" />
            Open API Docs Manually
          </Button>
        </CardContent>
      </Card>
    </div>
  );
}
