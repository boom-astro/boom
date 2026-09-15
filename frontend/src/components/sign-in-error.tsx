import { Link } from "react-router-dom";

export function SignInError({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="w-full max-w-lg mx-auto p-4">
      <div className="rounded-md border border-destructive/40 bg-destructive/10 p-4">
        <h1 className="font-medium mb-1">{title}</h1>
        <p className="text-sm text-muted-foreground">{children}</p>
        <Link to="/login" className="mt-3 inline-block text-sm underline">
          Back to login
        </Link>
      </div>
    </div>
  );
}
