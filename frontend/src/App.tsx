import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Route, Routes } from "react-router-dom";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { ThemeProvider } from "@/components/ThemeProvider";
import { Layout } from "@/components/Layout";
import { NotificationProvider } from "@/providers/NotificationProvider";
import Portfolio from "./pages/Portfolio";
import PropertyDetail from "./pages/PropertyDetail";
import DraftReply from "./pages/DraftReply";
import Compare from "./pages/Compare";
import Inbox from "./pages/Inbox";
import NotFound from "./pages/NotFound.tsx";

const queryClient = new QueryClient();

const App = () => (
  <QueryClientProvider client={queryClient}>
    <ThemeProvider>
      <TooltipProvider>
        <Toaster />
        <Sonner />
        <BrowserRouter>
          <NotificationProvider>
            <Routes>
              <Route element={<Layout />}>
                <Route path="/" element={<Portfolio />} />
                <Route path="/properties/:id" element={<PropertyDetail />} />
                <Route path="/inbox" element={<Inbox />} />
                <Route path="/draft-reply" element={<DraftReply />} />
                <Route path="/compare" element={<Compare />} />
              </Route>
              <Route path="*" element={<NotFound />} />
            </Routes>
          </NotificationProvider>
        </BrowserRouter>
      </TooltipProvider>
    </ThemeProvider>
  </QueryClientProvider>
);

export default App;
