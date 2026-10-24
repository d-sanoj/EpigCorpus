/* The small set of controls every screen is built from. One place for their
 * sizes, states and focus rings keeps the whole app consistent. */
import {
  forwardRef,
  useEffect,
  useRef,
  type AnchorHTMLAttributes,
  type ButtonHTMLAttributes,
  type ReactNode,
  type RefObject,
} from "react";

export function cx(...names: (string | false | null | undefined)[]): string {
  return names.filter(Boolean).join(" ");
}

type Variant = "primary" | "secondary" | "ghost" | "accent";
type Size = "sm" | "md" | "lg";

const base =
  "inline-flex select-none items-center justify-center gap-2 whitespace-nowrap rounded-lg font-medium " +
  "transition-[background-color,border-color,color,box-shadow,transform] duration-150 " +
  "active:scale-[0.98] disabled:pointer-events-none disabled:opacity-45 " +
  "aria-disabled:pointer-events-none aria-disabled:opacity-45";

const variants: Record<Variant, string> = {
  primary: "bg-ink text-ink-fg shadow-[0_1px_2px_#0000001f,inset_0_1px_0_#ffffff1f] hover:opacity-90",
  accent: "bg-accent text-white shadow-[0_1px_2px_#0000001f,inset_0_1px_0_#ffffff26] hover:brightness-110",
  secondary:
    "border border-line bg-surface text-fg shadow-[0_1px_0_#1b1a170a] hover:border-line-strong hover:bg-surface-2",
  ghost: "text-fg-2 hover:bg-surface-2 hover:text-fg",
};

const sizes: Record<Size, string> = {
  sm: "h-8 px-3 text-[0.8rem]",
  md: "h-9 px-3.5 text-sm",
  lg: "h-10 px-4 text-sm",
};

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: Variant;
  size?: Size;
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(function Button(
  { variant = "secondary", size = "md", className, type = "button", ...props },
  ref,
) {
  return (
    <button
      ref={ref}
      type={type}
      className={cx(base, variants[variant], sizes[size], className)}
      {...props}
    />
  );
});

interface LinkButtonProps extends AnchorHTMLAttributes<HTMLAnchorElement> {
  variant?: Variant;
  size?: Size;
  disabled?: boolean;
}

/** A link that looks like a button (downloads). Disabled, it stops being a link. */
export function LinkButton({
  variant = "secondary",
  size = "md",
  className,
  disabled,
  href,
  ...props
}: LinkButtonProps) {
  return (
    <a
      className={cx(base, variants[variant], sizes[size], className)}
      href={disabled ? undefined : href}
      aria-disabled={disabled || undefined}
      {...props}
    />
  );
}

interface IconButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  label: string;
  active?: boolean;
  size?: "sm" | "md";
}

export const IconButton = forwardRef<HTMLButtonElement, IconButtonProps>(function IconButton(
  { label, active, size = "md", className, type = "button", children, ...props },
  ref,
) {
  return (
    <button
      ref={ref}
      type={type}
      aria-label={label}
      title={label}
      className={cx(
        "inline-flex flex-none items-center justify-center rounded-lg text-fg-2 transition-colors",
        "hover:bg-surface-2 hover:text-fg active:scale-95 disabled:opacity-40",
        size === "md" ? "size-9" : "size-8",
        active && "bg-surface-2 text-fg",
        className,
      )}
      {...props}
    >
      {children}
    </button>
  );
});

interface SegmentedProps<T extends string> {
  label: string;
  value: T;
  options: { value: T; label: ReactNode; title?: string }[];
  onChange: (value: T) => void;
  size?: "sm" | "md";
  className?: string;
}

/** A row of mutually exclusive choices, as a radio group. */
export function Segmented<T extends string>({
  label,
  value,
  options,
  onChange,
  size = "md",
  className,
}: SegmentedProps<T>) {
  return (
    <div
      role="radiogroup"
      aria-label={label}
      className={cx("flex rounded-lg border border-line bg-surface-2 p-0.5", className)}
    >
      {options.map((option) => {
        const selected = option.value === value;
        return (
          <button
            key={option.value}
            type="button"
            role="radio"
            aria-checked={selected}
            title={option.title}
            onClick={() => onChange(option.value)}
            className={cx(
              "flex flex-1 items-center justify-center gap-1.5 rounded-md font-medium transition-all",
              size === "md" ? "h-8 px-2.5 text-[0.8rem]" : "h-7 px-2 text-xs",
              selected
                ? "bg-surface text-fg shadow-[0_1px_2px_#0000001a] ring-1 ring-line"
                : "text-muted hover:text-fg",
            )}
          >
            {option.label}
          </button>
        );
      })}
    </div>
  );
}

export function Badge({ children, tone = "neutral" }: { children: ReactNode; tone?: "neutral" | "accent" }) {
  return (
    <span
      className={cx(
        "inline-flex h-5 min-w-5 items-center justify-center rounded-full px-1.5 text-[0.7rem] font-semibold tabular-nums",
        tone === "accent" ? "bg-accent text-white" : "bg-surface-3 text-fg-2",
      )}
    >
      {children}
    </span>
  );
}

export function Kbd({ children }: { children: ReactNode }) {
  return (
    <kbd className="inline-flex h-5 items-center rounded border border-line bg-surface-2 px-1.5 font-sans text-[0.68rem] font-medium text-muted">
      {children}
    </kbd>
  );
}

/** Close something when a click lands outside it or Escape is pressed. */
export function useDismiss(
  ref: RefObject<HTMLElement | null>,
  open: boolean,
  close: () => void,
) {
  const latest = useRef(close);
  latest.current = close;
  useEffect(() => {
    if (!open) return;
    const onPointer = (event: PointerEvent) => {
      if (ref.current && !ref.current.contains(event.target as Node)) latest.current();
    };
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") latest.current();
    };
    document.addEventListener("pointerdown", onPointer);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("pointerdown", onPointer);
      document.removeEventListener("keydown", onKey);
    };
  }, [ref, open]);
}
