import { cn } from "../../lib/utils";

interface CardProps extends React.HTMLAttributes<HTMLDivElement> { }

export function Card({ className, ...props }: CardProps) {
    return (
        <div
            className={cn(
                "rounded-md border border-surface-border bg-surface text-text-primary shadow-sm transition-shadow hover:shadow-md",
                className
            )}
            {...props}
        />
    );
}

export function CardHeader({ className, ...props }: CardProps) {
    return (
        <div
            className={cn("flex flex-col space-y-1.5 p-6", className)}
            {...props}
        />
    );
}

export function CardTitle({ className, ...props }: CardProps) {
    return (
        <h3
            className={cn("font-normal text-text-primary tracking-tight", className)}
            {...props}
        />
    );
}

export function CardContent({ className, ...props }: CardProps) {
    return <div className={cn("p-6 pt-0", className)} {...props} />;
}
