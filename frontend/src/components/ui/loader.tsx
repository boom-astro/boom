import { ComponentProps } from "react"
import { Spinner } from "@/components/ui/spinner.tsx";

function Loader({ className, ...props }: ComponentProps<"svg">) {
  return (
    <div className="flex h-screen items-center justify-center">
      <Spinner className={className} {...props} />
    </div>
)
}

export { Loader }
