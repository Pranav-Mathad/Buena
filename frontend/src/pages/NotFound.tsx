import { useEffect } from "react";
import { Link, useLocation } from "react-router-dom";
import { ArrowLeft } from "lucide-react";
import { useDocumentTitle } from "@/hooks/use-document-title";
import { Mark } from "@/components/ui-bits";

const NotFound = () => {
  const location = useLocation();
  useDocumentTitle("Page not found");

  useEffect(() => {
    console.error("404 Error: User attempted to access non-existent route:", location.pathname);
  }, [location.pathname]);

  return (
    <main className="min-h-screen flex items-center justify-center px-6 animate-fade-in">
      <div className="max-w-md text-center">
        <Mark className="w-10 h-10 mx-auto" />
        <p className="eyebrow mt-8">404</p>
        <h1 className="display mt-3 text-[30px]">There is nothing at this address.</h1>
        <p className="mt-4 text-[15px] text-muted-foreground leading-relaxed">
          <code className="font-mono text-[13px] bg-foreground/[0.06] px-1.5 py-0.5 rounded">
            {location.pathname}
          </code>{" "}
          is not a page in Keystone.
        </p>
        <Link
          to="/"
          className="mt-8 inline-flex items-center gap-2 rounded-full bg-primary text-primary-foreground px-5 py-2.5 text-[14px] font-medium transition-all duration-200 ease-out-soft hover:-translate-y-px hover:shadow-pop"
        >
          <ArrowLeft className="w-4 h-4" />
          Back to the portfolio
        </Link>
      </div>
    </main>
  );
};

export default NotFound;
