import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import {
  Field,
  FieldDescription,
  FieldGroup,
  FieldLabel,
} from "@/components/ui/field"
import { Link } from 'react-router-dom'
import { Input } from "@/components/ui/input"

// Release mode flag - set VITE_PRERELEASE_MODE=true at build time to restrict signup
const PRERELEASE_MODE = import.meta.env.VITE_PRERELEASE_MODE === 'true';

interface LoginFormProps extends React.ComponentProps<"div"> {
  email?: string;
  password?: string;
  onEmailChange?: (e: React.ChangeEvent<HTMLInputElement>) => void;
  onPasswordChange?: (e: React.ChangeEvent<HTMLInputElement>) => void;
  onSubmit?: (e?: React.FormEvent) => void;
  loading?: boolean;
  error?: string | null;
}

export function LoginForm({
  className,
  email,
  password,
  onEmailChange,
  onPasswordChange,
  onSubmit,
  loading,
  error,
  ...props
}: LoginFormProps) {
  return (
    <div className={cn("flex flex-col gap-6", className)} {...props}>
      <Card>
        <CardHeader>
          <CardTitle>Login to your account</CardTitle>
          <CardDescription>
            Enter your email below to login to your account
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={onSubmit}>
            <FieldGroup>
              <Field>
                <FieldLabel htmlFor="email">Email</FieldLabel>
                <Input
                  id="email"
                  type="text"
                  placeholder="your email"
                  required
                  value={email}
                  onChange={onEmailChange}
                />
              </Field>
              <Field>
                <div className="flex items-center">
                  <FieldLabel htmlFor="password">Password</FieldLabel>
                  <Link
                    to="/forgot-password"
                    className="ml-auto inline-block text-sm underline-offset-4 hover:underline"
                  >
                    Forgot your password?
                  </Link>
                </div>
                <Input id="password" type="password" required value={password} onChange={onPasswordChange} />
              </Field>
              <Field>
                <Button type="submit" disabled={!!loading}>{loading ? 'Signing in…' : 'Login'}</Button>
                {!PRERELEASE_MODE ? (
                  <FieldDescription className="text-center">
                    Don&apos;t have an account? <Link to="/signup" className="underline">Sign up</Link>
                  </FieldDescription>
                ) : (
                  <FieldDescription className="text-center pt-4 italic text-sm text-yellow-500">
                    Account creation will be enabled when LSST alerts are publicly available!
                  </FieldDescription>
                )}
              </Field>
            </FieldGroup>
          </form>
          {error && <div className="text-red-600 mt-2">{error}</div>}
        </CardContent>
      </Card>
    </div>
  )
}
